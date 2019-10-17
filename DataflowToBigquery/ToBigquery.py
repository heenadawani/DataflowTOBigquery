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
global schema

def run(argv=None):
    os.system("gsutil cp gs://heena_dawani/dummy.csv .")
    df = pd.read_csv("dummy.csv")
    df.to_csv("dummy1.csv",index=False,sep = "|")
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input', dest='input', required=False,
        help='Input file to read.  This can be a local file or '
             'a file in a Google Storage Bucket.',
        default='dummy1.csv')
    parser.add_argument('--output', dest='output', required=False,
                        help='Output BQ table to write results to.',
                        default='certiphication:heena_dawani.games')

    known_args, pipeline_args = parser.parse_known_args(argv)
    schema = 'Rank:INTEGER,Name:STRING,Platform:STRING,Year:INTEGER,Genre:STRING,Publisher:STRING,NA_Sales:FLOAT,EU_Sales:FLOAT,JP_Sales:FLOAT,Other_Sales:FLOAT'
    p = beam.Pipeline(options=PipelineOptions(pipeline_args))

    #data_ingestion = DataTransformation()

    class DataTransformation(beam.DoFn):
        def parse_method(self, element):
            element = element.split('|')
            length = len(element)
            for csv_row in range(length) :
                values = element
                value = {}
                if values[0] == "":
                    values[0] = "N/A"
                if values[1] == "":
                    values[1] = "N/A"
                if values[2] == "":
                    values[2] = "N/A"
                if values[3] == "":
                    values[3] = "N/A"
                if values[4] == "":
                    values[4] = "N/A"
                if values[5] == "":
                    values[5] = "N/A"
                if values[6] == "":
                    values[6] = "N/A"
                if values[7] == "":
                    values[7] = "N/A"
                if values[8] == "":
                    values[8] = "N/A"
                if values[9] == "":
                    values[9] = "N/A"
                
                value['Rank'] = values[0]
                value['Name'] = values[1]
                value['Platform'] = values[2]
                value['Year'] = values[3]
                value['Genre'] = values[4]
                value['Publisher'] = values[5]
                value['NA_Sales'] = values[6]
                value['EU_Sales'] = values[7]
                value['JP_Sales'] = values[8]
                value['Other_Sales'] = values[9]
            print(value)
            return value
            
    
    quote = (p
    | 'Read From Text' >> beam.io.ReadFromText(known_args.input,
                                                skip_header_lines=1)
    | 'String to BigQuery Row' >> beam.ParDo(DataTransformation())
            )


    quote | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
            known_args.output,
            schema=schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)
    p.run().wait_until_finish()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()