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


class DataTransformation:
    global schema
    schema = 'Rank:INTEGER,Name:STRING,Platform:STRING,Year:INTEGER,Genre:STRING,Publisher:STRING,NA_Sales:NUMERIC,EU_Sales:NUMERIC,JP_Sales:NUMERIC,Other_Sales:NUMERIC'

  #  def __init__(self):
   #     dir_path = os.path.dirname(os.path.realpath(__file__))
    #    self.schema_str = ''

    def parse_method(self, string_input):

        #field_map = [f for f in schema.fields]

        # Use a CSV Reader which can handle quoted strings etc.
        reader = csv.reader(string_input.split('\n'))
        for csv_row in reader:
            values = [x.encode('utf8') for x in csv_row]
            row = []
            i = 0
            for value in csv_row:
                if value == 'NaN':
                    value = 'Null'
                row.append(value)
        return ",".join(row)


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input', dest='input', required=False,
        help='Input file to read.  This can be a local file or '
             'a file in a Google Storage Bucket.',
        default='gs://heena_dawani/gamesdata_cleaned.csv')
    parser.add_argument('--output', dest='output', required=False,
                        help='Output BQ table to write results to.',
                        default='certiphication:heena_dawani.heena')

    known_args, pipeline_args = parser.parse_known_args(argv)
    data_ingestion = DataTransformation()

    p = beam.Pipeline(options=PipelineOptions(pipeline_args))

    (p
     | 'Read From Text' >> beam.io.ReadFromText(known_args.input,
                                                skip_header_lines=1)
     | 'String to BigQuery Row' >> beam.Map(lambda s:
                                            data_ingestion.parse_method(s))
     | 'Write to BigQuery' >> beam.io.Write(
        beam.io.BigQuerySink(
            known_args.output,
            schema=schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)))
    p.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()