import os
os.system('sudo apt install -y python-pip')
os.system('sudo pip install pandas')
os.system('sudo pip install apache-beam[gcp]')
os.system('sudo pip install gcsfs')
os.system('sudo pip install google-cloud-bigquery')
import google.auth as gauth
import apache_beam as beam
import sys
import csv
if __name__ == '__main__':
   p = beam.Pipeline(argv=sys.argv)
   q= beam.Pipeline(argv=sys.argv)
   from zipfile import ZipFile
   import pandas as pd
   import gcsfs
   fs = gcsfs.GCSFileSystem(project='aai-west-training')
   with fs.open('aai-west-training-data/Games Data.zip') as f:
        with ZipFile(f, 'r') as zip: 
                zip.extractall()
   df=pd.read_csv("Games Data/P9-ConsoleGames.csv")
   df1=pd.read_csv("Games Data/P9-ConsoleDates.csv")
   df= df.dropna(axis=0, subset=['Name'])
   df['Platform'] = df['Platform'].fillna('N/A')
   df['Publisher'] = df['Publisher'].fillna('N/A')
   df['Year'] = df['Year'].fillna(0.0)
   cols=["NA_Sales","EU_Sales","JP_Sales","Other_Sales"]
   df[cols]=df[cols].fillna(0.0)
   df1['Comment'] = df1['Comment'].fillna('N/A')
   df1['Discontinued']=df1['Discontinued'].fillna('0')
   newdict=df.to_dict('records')
   dict1=df1.to_dict('records')
from apache_beam.io.gcp.internal.clients import bigquery
#table of games csv
table_spec = bigquery.TableReference(
    projectId='aai-west-training',
    datasetId='neetidbdataflowassign',
    tableId='ConsoleGames')
#table of Dates csv
table_spec1 = bigquery.TableReference(
    projectId='aai-west-training',
    datasetId='neetidbdataflowassign',
    tableId='ConsoleDates')
#Gamestable Schema
table_schema = { "fields": [
  {
    "name": "Rank", 
    "type": "INTEGER"
  }, 
  { "name": "Name", 
    "type": "STRING"
  }, 
  {
    "name": "Platform", 
    "type": "STRING"
  }, 
  {
    "name": "Year", 
    "type": "FLOAT"
  }, 
  { 
    "name": "Genre", 
    "type": "STRING"
  }, 
  {
    "name": "Publisher", 
    "type": "STRING"
  }, 
  { 
    "name": "NA_Sales", 
    "type": "FLOAT"
  }, 
  {
    "name": "EU_Sales", 
    "type": "FLOAT"
  }, 
  {
    "name": "JP_Sales", 
    "type": "FLOAT"
  }, 
  {
    "name": "Other_Sales", 
    "type": "FLOAT"
  }
]}
#Datestable Schema
table_schema1 = { "fields":  [
 {
    "name": "Platform", 
    "type": "STRING"
  },
 {
    "name": "FirstRetailAvailability", 
    "type": "DATE"
  },
 {
    "name": "Discontinued", 
    "type": "DATE"
  },
 {
    "name": "UnitsSoldMillions", 
    "type": "FLOAT"
  },
 {
    "name": "Comment", 
    "type": "STRING"
  },
]}

lines = p| beam.Create(newdict)
lines| beam.io.WriteToBigQuery(
   table_spec,
   schema=table_schema,
   write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
   create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
line1=q|beam.Create(dict1)
line1| beam.io.WriteToBigQuery( 
   table_spec1, 
   schema=table_schema1, 
   write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE, 
   create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
p.run().wait_until_finish()
q.run().wait_until_finish()