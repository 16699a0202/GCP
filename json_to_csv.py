import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

parser=argparse.ArgumentParser()
parser.add_argument(
    '--input',
    dest='input',
    required=True,
    default='MOCK_DATA(1).json'
)

parser.add_argument(
    '--output',
    dest='output',
    required=True,
    default='output_data.txt'
)

known_args,pipeline_args=parser.parse_known_args()

p1=beam.Pipeline(options=PipelineOptions(pipeline_args))

def split_row(row):
    l=row.split(',') #['{"id":1', '"first_name":"Ynes"', '"last_name":"MacCarter"', '"email":"ymaccarter0@wikia.com"', '"gender":"Genderfluid"}']
    d={}
    for i in l:
        s=i.split(":") #[{"id",'1']
        d[s[0]]=s[1]
    return list(d.values()) #['1', '"Ynes"', '"MacCarter"', '"ymaccarter0@wikia.com"', '"Genderfluid"}']

def join_row(row):
    row= ','.join(row) #1,"Ynes","MacCarter","ymaccarter0@wikia.com","Genderfluid"}
    return row[:-1] # 1,"Ynes","MacCarter","ymaccarter0@wikia.com","Genderfluid"

doctorcount=(
    p1
    | 'Read from a file' >> beam.io.ReadFromText(known_args.input)#{"id":1,"first_name":"Ynes","last_name":"MacCarter","email":"ymaccarter0@wikia.com","gender":"Genderfluid"}
    | beam.Map(split_row)
    | beam.Map(join_row)
    | 'write to a file' >>beam.io.Write(beam.io.BigQuerySink(known_args.output,schema='id:INTEGER,first_name:STRING,last_name:STRING,email:STRING,gender:STRING'))
)

p1.run().wait_until_finish()

#python -m json_to_csv.py --input "MOCK_DATA (1).json" --output "output_data.txt"