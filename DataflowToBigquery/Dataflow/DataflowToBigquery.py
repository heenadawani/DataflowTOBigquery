"""
To run this file you need to have a csv in the bucket on which the preprocessing is to be performed.

"""
from __future__ import absolute_import
import logging
import datetime
import argparse
import logging
import re
import os
from past.builtins import unicode
import apache_beam as beam
import pandas as pd
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.metrics import Metrics
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--output',
                    required=True,
                    help=('Output BigQuery table for results specified as: \
                    PROJECT:DATASET.TABLE'),
                    default='certiphication.heena_dawani.heena')
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    p = beam.Pipeline(options=pipeline_options)

    class Transaction(beam.DoFn):
        def process(self, element):
            
            element = element.split('|')
            values= element

            length= len(values)
            row = []
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
                    values[6] = "0"
                if values[7] == "":
                    values[7] = "0"
                if values[8] == "":
                    values[8] = "0"
                if values[9] == "":
                    values[9] = "0"
                
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
            return value
            

    data_from_source = (p
                        | 'Read the source file' >> ReadFromText('/home/heena_dawani/dummy1.csv', skip_header_lines=1)
                        | 'Clean the items' >> beam.ParDo(Transaction())

                        )
                        
    
    data_from_source | 'Write' >> beam.io.WriteToBigQuery(
                    known_args.output,
                    schema='Rank:INTEGER,Name:STRING,Platform:STRING,Year:FLOAT,Genre:STRING,Publisher:STRING,NA_Sales:FLOAT,EU_Sales:FLOAT,JP_Sales:FLOAT,Other_Sales:FLOAT',
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)
                    
                    

    result = p.run()
    result.wait_until_finish()

if __name__ == '__main__':
    logger = logging.getLogger().setLevel(logging.INFO)
    run()
"""

"""