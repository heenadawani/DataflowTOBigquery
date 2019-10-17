import apache_beam as beam
import sys

PROJECT='certification'
BUCKET='heena_dawani'
schema = 'Rank:INTEGER,Name:STRING,Platform:STRING,Year:INTEGER,Genre:STRING,Publisher:STRING,NA_Sales:FLOAT,EU_Sales:FLOAT,JP_Sales:FLOAT,Other_Sales:FLOAT'

class Split(beam.DoFn):

   def process(self, element):
      id, region = element.split(",")

      return [{
      'id': int(id),
      'region': region,
      }]
   """
   def parse_method(line):
        import csv
        reader = csv.reader(line.split('\n'))
        for csv_row in reader:
            values = [x.decode('utf8') for x in csv_row]
            row = []
            for value in csv_row:
                if value == 'NaN':
                    value = 'Null'
                row.append(value)

        return ",".join(row)
   """

def run():
   argv = [
      '--project={0}'.format(PROJECT),
      '--staging_location=gs://{0}/staging/'.format(BUCKET),
      '--temp_location=gs://{0}/staging/'.format(BUCKET),
      '--runner=DataflowRunner'
   ]

   p = beam.Pipeline(argv=argv)

   (p
      | 'ReadFromGCS' >> beam.io.textio.ReadFromText('gs://{0}/staging/dummy.csv'.format(BUCKET))
      | 'ParseCSV' >> beam.ParDo(Split())
      | 'WriteToBigQuery' >> beam.io.WriteToBigQuery('{0}:test.dummy'.format(PROJECT), schema=schema)
   )

   p.run()

if __name__ == '__main__':
   run()