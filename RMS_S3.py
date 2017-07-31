# Uses Luigi to connect to an API, store to a Postgres RDB and update HTML file with charts
# Gary Pate 2017

import luigi
import luigi.postgres as lpg
from luigi.contrib.s3 import S3Target, S3Client
import requests
import datetime
import os
import yaml
import re
import json
import pandas as pd
from Pcred import pcreds, awscreds
from boto_conn import boto3Conn, s3open, s3delete
from RMS_utils import cleanUp, DBconnect, RMS_Api_config, parseEvent, parseIncident, endIncidents, dfEvent, dfIncident, tseriesPlotly, gannPlotly, normalizeDF, clustPlot, threePlotly, updateDivs, updateHTML
from boto.s3.connection import Bucket, Key


# Delete any data on RDB that is older than a day old
class delete23(luigi.Task):

    def requires(self):
        return []

    def output(self):
        return luigi.LocalTarget(os.getcwd() + r"/flags/Delete23_run.txt")

    def run(self):

        conn = DBconnect(pcreds)
        cur = conn.cursor()

        sql = ("""
                DELETE FROM
                events
                WHERE
                dat < now() - '23 hour' :: interval;

                DELETE FROM
                incidents
                WHERE
                dat_str < now() - '23 hour' :: interval;
                """)

        cur.execute(sql)
        conn.commit()

        with self.output().open("w") as id_nums:
            id_nums.write("23 hours of data culled")

# Checks the existing keys in database to ensure no duplication
class existCheck(luigi.Task):

    def requires(self):
        return []

    # Outputs a flag dummy file
    def output(self):
        return luigi.LocalTarget(os.getcwd() + r"/flags/list.txt")

    def run(self):

        conn = DBconnect(pcreds)
        cur = conn.cursor()

        sql = ("""
                SELECT id, 'event' as class FROM events
                UNION
                SELECT id, 'incident' as class FROM incidents
                order by class
                """)

        cur.execute(sql)
        rows = cur.fetchall()

        with self.output().open("w") as id_nums:
            id_nums.write("id, type")
            for row in rows:
                id_nums.write("\n")
                id_nums.write("{}, {}".format(*row))

# API connection and saving data to S3 Bucket as txt
class APIfetch(luigi.Task):

    ur = luigi.Parameter()
    did_run = False

    def requires(self):
        return []

    # Sends outputs to S3
    def output(self):
        file = '{}_{:%Y-%m-%d %H+%M+%S}_data.txt'.format(self.ur, datetime.datetime.now())
        client = S3Client(awscreds[0], awscreds[1])
        return S3Target('s3://rmsapi/%s' % file, client=client)

    def run(self):
        # API connection
        def connections(urls, headers):
            conn = requests.get(urls, headers=headers)
            return conn.json()

        headers = RMS_Api_config['headers']
        urls = RMS_Api_config['urls']

        # Connects to the API twice
        for u in urls:
            self.ur = u[0]
            output = connections(u[1], headers)
            with self.output().open('w') as outfile:
                json.dump(output, outfile)

        self.did_run = True

    # Complete method required to prevent from running twice
    def complete(self):
        return self.did_run

# Parsing individual txt files into a cleaned and complete file
class dataWrangle(luigi.Task):

    def requires(self):
        return APIfetch(['event', 'incident'])

    def output(self):
        return [luigi.LocalTarget(os.getcwd() + r"/clean/Event_Clean.csv"),
                luigi.LocalTarget(os.getcwd() + r"/clean/Incident_Clean.csv")]

    # Connects to S3 bucket and checks file length
    def run(self):
        s3 = boto3Conn()
        s3content = s3.list_objects_v2(Bucket='rmsapi')['Contents']
        s3files = [s['Key'] for s in s3content]
        s3 = s3open()
        s3read = s3.Bucket('rmsapi')

        if len(s3files) > 59:
            # Creates data frames and assigns columns
            E_cols = ['id', 'etype', 'diff', 'lon', 'lat', 'bear', 'road', 'dat']
            I_cols = ['id', 'lon', 'lat', 'itype', 'street', 'dat_str', 'dat_end', 'ended']
            df_outE = pd.DataFrame(columns=E_cols)
            df_outI = pd.DataFrame(columns=I_cols)
            timelist = []

            # Iterates over files and stores the timestamp of the file
            for file in s3read.objects.all():
                key = file.key
                body = file.get()['Body'].read()
                timestamp = re.findall('[^_]+', key)
                timestamp = timestamp[1].replace('+', ':')
                timelist.append(timestamp)

                # Different methods for event and incident files
                if key.startswith('event'):
                    df_outE = parseEvent(body, timestamp, df_outE)
                if key.startswith('inciden'):
                    df_outI = parseIncident(body, timestamp, df_outI)

            # Output to csv file
            with self.output()[0].open('w') as csvfile:
                df_outE['diff'] = df_outE['diff'].astype(int)
                df_outE['id'] = pd.to_numeric(df_outE['id'])
                df_outE.to_csv(csvfile, columns=E_cols, index=False)

            with self.output()[1].open('w') as csvfile:
                # Create an end time stamp for data that has not actually ended yet for Gantt file
                df_outI = endIncidents(timelist, df_outI)
                df_outI['id'] = df_outI['id'].astype(int)
                df_outI.to_csv(csvfile, columns=I_cols, index=False)

            s3del = Bucket(s3delete(),'rmsapi')
            k = Key(s3del)

            # Delete all files in S3 folder
            for file in s3files:
                k.key = file
                s3del.delete_key(k)

# Creates the necessary parameters for the schema
class schemaAssemble(luigi.Task):

    tables = luigi.Parameter(default='')
    columns = luigi.Parameter(default='')
    cleancsv = luigi.Parameter(default='')

    def run(self):
        # Sets up the two schemas for the two uploads
        self.tables = ['events', "incidents"]
        self.columns = [[("id", "BIGINT"),
                         ("etype", "TEXT"),
                         ("diff", "INT"),
                         ("lon", "DOUBLE PRECISION"),
                         ("lat", "DOUBLE PRECISION"),
                         ("bear", "DOUBLE PRECISION"),
                         ("road", "TEXT"),
                         ("dat", "DATE")],
                        [("id", "INT"),
                         ("lon", "DOUBLE PRECISION"),
                         ("lat", "DOUBLE PRECISION"),
                         ("itype", "TEXT"),
                         ("street", "TEXT"),
                         ("dat_str", "DATE"),
                         ("dat_end", "DATE"),
                         ("ended", "TEXT")]
                        ]

        # Checks for primary keys that already exist
        with self.input()[1].open('r') as csvfile:
            df_dups = pd.DataFrame.from_csv(csvfile, header=0, index_col=None)
            tup_dup = tuple(list(df_dups['id']))

        list_outs = []
        # Iterates over the input and prepares them for the output
        outs = ['output1', 'output2']
        for k, (l, m) in enumerate(zip(self.input()[0], outs)):
            with l.open('r') as csvfile:
                cleaned_df = pd.DataFrame.from_csv(csvfile, header=0, index_col=None)
                # Takes the reverse of the data set that contains the existing keys
                cleaned_df = cleaned_df[~cleaned_df['id'].isin(tup_dup)]
                self.cleancsv = cleaned_df.to_dict(orient='list')
                # Arranges the data with the schema and outputs to a another file
                schema = {'tables': self.tables[k], 'columns': self.columns[k], 'data': self.cleancsv}
                list_outs.append([schema, m])

        # Outputs the JSON
        for l in list_outs:
            with self.output()[l[1]].open('w') as txtfile:
                json.dump(l[0], txtfile)

    def requires(self):
        return [dataWrangle(), existCheck(), delete23()]

    def output(self):
        return {'output1': luigi.LocalTarget(os.getcwd() + r'/clean/Schema_1.txt'),
                'output2': luigi.LocalTarget(os.getcwd() + r'/clean/Schema_2.txt')}

# Takes the data ready to be uploaded and sends it to the task that performs the RDB upload
class spoolQuery(luigi.Task):

    tables = luigi.Parameter(default='')
    columns = luigi.Parameter(default='')
    cleancsv = luigi.Parameter(default='')
    log = luigi.Parameter(default='')

    def requires(self):
        return schemaAssemble()

    # Outputs a flag dummy file
    def output(self):
        return luigi.LocalTarget(os.getcwd() + r"/flags/Spool_run.txt")

    def run(self):
        ins = ['output1', 'output2']
        def tupCols(parameter):
            return [tuple(l) for l in parameter]

        # Takes each of the inputs and sends them to the tableCopy task
        for l in ins:
            with self.input()[l].open('r') as jsonfile:
                dict = json.load(jsonfile)
                self.tables = dict['tables']
                self.columns = dict['columns']
                self.columns = tupCols(self.columns)
                self.cleancsv = dict['data']
                # Send twice to the task
                yield PGREScopyTable(table=self.tables, columns=self.columns, cleancsv=self.cleancsv)

        # Outputs a flag file
        with self.output().open("w") as graph_log:
            graph_log.write("Spool run")

# Task for copying to PostGRES tables
class PGREScopyTable(lpg.CopyToTable):

    table = luigi.Parameter()
    columns = luigi.Parameter()
    cleancsv = luigi.Parameter()

    # Database credentials
    database, user, password, host = [p for p in pcreds]

    def rows(self):
        self.columns = eval(self.columns)
        loaded = yaml.load(self.cleancsv)
        cols = [i[0] for i in self.columns]

        for l in range(len(loaded[cols[0]])):
            # Yields each row for the data to be send to the RDS
            yield (loaded[i][l] for i in cols)

# Task for retrieved data from PostGRES
class graphQuery(luigi.Task):
    # Save the retrieved data
    def output(self):
        return [luigi.LocalTarget(os.getcwd() + r"/clean/Event_All.csv"),
                luigi.LocalTarget(os.getcwd() + r"/clean/Incident_All.csv")]

    def requires(self):
        return spoolQuery()

    def run(self):
        # Database connection
        conn = DBconnect(pcreds)
        cur = conn.cursor()
        # Query RDB
        for i, table in enumerate(["events", "incidents"]):
            cur.execute("SELECT * FROM %s" % table)
            rows = cur.fetchall()
            with self.output()[i].open("w") as id_nums:
                for row in rows:
                    id_nums.write(','.join(str(s) for s in row) + '\n')

# Task for generating Plotly charts and updating HTML file
class graphing(luigi.Task):
    def requires(self):
        return graphQuery()

    # Outputs a flag dummy file
    def output(self):
        return luigi.LocalTarget(os.getcwd() + r"/flags/Graphing_run.txt")

    def run(self):

        with self.output().open("w") as graph_log:
            graph_log.write("Graph Log")

        # Create dataframe
        df_mean = pd.DataFrame(columns=['time', 'mean'])

        with self.input()[0].open('r') as csvfile:                                        # retrieve one at a time
            df_ev = pd.DataFrame.from_csv(csvfile, header=0, index_col=None)
            # Method for processing event data and traffic mean dataframe
            df_ev, df_mean = dfEvent(df_ev, df_mean)

        with self.input()[1].open('r') as csvfile:                                        # retrieve one at a time
            df_in = pd.DataFrame.from_csv(csvfile, header=0, index_col=None)
            df_in.columns = ['id', 'lon', 'lat', 'itype', 'street', 'dat_str', 'dat_end', 'ended']
            # Method for processing incident dataframe
            df_gan = dfIncident(df_in)


        orig_divs = []
        # Generate the three charts and append to a list
        orig_divs.append(tseriesPlotly(df_mean))
        orig_divs.append(gannPlotly(df_gan))
        orig_divs.append(threePlotly(clustPlot(df_ev)))

        # delete the generated data files
        cleanUp([r"/flags/", r"/clean/"])
        divnames = ['graph-1', 'graph-2', 'graph-3']

        # Access the S3 bucket to update HTML
        s3 = s3open()
        obj = s3.Object('datafolio', 'luigi.html')
        body = obj.get()['Body'].read().decode('utf-8')
        timediv = 'Traffic data last updated on: {:%a-%d-%b %H:%M %p}</h3>'.format(datetime.datetime.now())
        div_list = updateDivs(divnames, orig_divs)
        obj.put(Body=updateHTML(body, div_list, timediv), ContentType='text/html')

if __name__ == '__main__':
    luigi.run(main_task_cls=graphing)



