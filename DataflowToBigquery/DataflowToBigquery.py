from __future__ import absolute_import
import argparse
import csv
import logging
import os

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import parse_table_schema_from_json
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import pandas as pd


class DataTransformation(beam.DoFn):
    global schema
    schema = 'Rank:INTEGER,Name:STRING,Platform:STRING,Year:INTEGER,Genre:STRING,Publisher:STRING,NA_Sales:FLOAT,EU_Sales:FLOAT,JP_Sales:FLOAT,Other_Sales:FLOAT'

  #  def __init__(self):
   #     dir_path = os.path.dirname(os.path.realpath(__file__))
    #    self.schema_str = ''

    def parse_method(self, string_input):
        reader = string_input.split('|')
        #print(type(reader))
        #row = []
        for csv_row in reader:
            values = [x.encode('utf8') for x in csv_row]
            value = {}
            value['Rank'] = int(values[0],10)
            value['Name'] = values[1]
            if values[2] == "":
                values[2] = "N/A"
            value['Platform'] = values[2]
            value['Year'] = int(values[3],10)
            value['Genre'] = values[4]
            value['Publisher'] = values[5]
            if values[6] == "":
                values[6] == 0.0
            if values[7] == "":
                values[7] == 0.0
            if values[8] == "":
                values[8] == "0"
            if values[9] == "":
                values[9] == 0.0
            value['NA_Sales'] = float(values[6])
            value['EU_Sales'] = float(values[7])
            value['JP_Sales'] = (values[8])
            value['Other_Sales'] = float(values[9])
        return value

def run(argv=None):
    os.system("gsutil cp gs://heena_dawani/P9-ConsoleGames.csv .")
    df = pd.read_csv("P9-ConsoleGames.csv")
    df.to_csv("dummy1.csv",index=False,sep = "|")
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input', dest='input', required=False,
        help='Input file to read.  This can be a local file or '
             'a file in a Google Storage Bucket.',
        default='dummy1.csv')
    parser.add_argument('--output', dest='output', required=False,
                        help='Output BQ table to write results to.',
                        default='certiphication:heena_dawani.heena')

    known_args, pipeline_args = parser.parse_known_args(argv)
    data_ingestion = DataTransformation()

    p = beam.Pipeline(options=PipelineOptions(pipeline_args))

    quote = (p
     | 'Read From Text' >> beam.io.ReadFromText(known_args.input,
                                                skip_header_lines=1)
     | 'String to BigQuery Row' >> beam.Map(lambda s:
                                            data_ingestion.parse_method(s)))
    quote | 'Write to BigQuery' >> beam.io.Write(
        beam.io.BigQuerySink(
            known_args.output,
            schema=schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
    p.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
