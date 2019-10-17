from __future__ import absolute_import

import argparse
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

import pandas as pd
data = pd.read_csv("P9-ConsoleGames.csv",skiprows=[1],header=None)

table_spec = 'certification:heena_dawani.heena'

table_schema = {'fields': [
    {'name': 'Rank', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'Name', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'Platform', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'Year', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'Genre', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'Publisher', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'NA_Sales', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'EU_Sales', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'JP_Sales', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'Other_Sales', 'type': 'FLOAT', 'mode': 'NULLABLE'}
]}
p = beam.Pipeline(argv=sys.argv)
quotes = p | beam.Create(data)
quotes | beam.io.WriteToBigQuery(
    table_spec,
    schema=table_schema,
    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)