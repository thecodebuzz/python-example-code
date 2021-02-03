import pandas as pd
df = pd.read_csv (r'gs://test-bucket-13/Input/Adress.csv')
df.to_json (r'gs://test-bucket-13/output/temp/adress.json')


google-cloud-storage==1.28.1
pandas==1.0.3
smart-open==2.0.0


python3 csv-json-pandas-dataflow.py \ --project test1\ --runner DataflowRunner --staging_location 
gs://test-bucket-13/\ --temp_location gs://test-bucket-13/\ --template_location gs://test-bucket-13/MyTemplate\ --save_main_session True --input  gs:/
/test-bucket-13/Input/Adress.csv  --output gs://test-bucket-13/output/temp/addressfromgcp.json
  
  
 

import pandas as pd
import argparse
import json
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import storage
from smart_open import open
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= 'test1-123.json'



class DataflowPipeline(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--inputFilePath', type=str, default='gs://test-bucket-13/Input/Adress.csv')
        parser.add_argument('--outputFilePath', type=str, default='gs://test-bucket-13/output/temp/addressfromapp.json')

class ConvertCSVToJson(beam.DoFn):

    def __init__(self, inputFilePath,outputFilePath):
        self.inputFilePath = inputFilePath
        self.outputFilePath = outputFilePath

    def start_bundle(self):
        self.client = storage.Client()

    def process(self, something):
       df = pd.read_csv (self.inputFilePath)
       df.to_json (self.outputFilePath)


def run(argv=None):
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    dataflow_options = pipeline_options.view_as(DataflowPipeline)

    with beam.Pipeline(options=pipeline_options) as pipeline:
        (pipeline
         | 'Start' >> beam.Create([None])
         | 'Convert CSV to JSON' >> beam.ParDo(ConvertCSVToJson(dataflow_options.inputFilePath, dataflow_options.outputFilePath))
         )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()





