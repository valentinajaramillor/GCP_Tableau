# Copyright 2018 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Dataflow pipeline that reads 1-n files and their schemas from GCS, 
transforms their content, and writes it to BigQuery tables.
To run this script, you will need Python packages listed in requirements.txt.
You can easily install them with virtualenv and pip by running these commands:
    virtualenv env
    source ./env/bin/activate
    pip install -r requirements.txt
To get documentation on the script options run:
    python dataflow_python_examples/data_ingestion_configurable.py --help
"""

import argparse
import json
import logging
import os
import re
from google.cloud import storage
import apache_beam as beam


"""Function to read data files from Cloud Storage
        Args:
            bucket_name: Name of the GCS bucket where files are stored
            path_file: The schema of the data in json format
        Returns:
            A dict mapping BigQuery column names as keys to the corresponding value
            parsed from string_input.
         """
def get_file_gcs(bucket_name, path_file):
    # Create Google Cloud Storage Client
    client = storage.Client()

    # Get bucket by name
    bucket = client.get_bucket(bucket_name)

    # Get the blob corresponding to a file in the bucket
    blob = bucket.get_blob(path_file)

    # Download the blob as string, to be transformed by the parse_method
    data = blob.download_as_string()
    return data


class DataIngestion:
    """A helper class which contains the logic to translate the file into
    a format BigQuery will accept."""

    @staticmethod
    def parse_method(string_input, schema_json):
        """This method translates a single line of comma separated values to a
        dictionary which can be loaded into BigQuery.
        Args:
            string_input: A comma separated list of values
            schema_json: The schema of the data in json format
        Returns:
            A dict mapping BigQuery column names as keys to the corresponding value
            parsed from string_input.
         """

        # Strip out carriage return, newline and quote characters.
        values = re.split(",", re.sub('\r\n', '', re.sub('"', '',
                                                         string_input)))

        # Get the fields name from the corresponding schema json file
        field_names = tuple(field['name'] for field in schema_json['fields'])

        # Create a row as a dictionary of field names and values
        row = dict(zip(field_names, values))
        return row


def run(argv=None):
    """The main function which creates the pipeline and runs it"""

    parser = argparse.ArgumentParser()
    parser.add_argument('--input-bucket',
                        dest='input_bucket',
                        required=True,
                        default='data-daimlr',
                        help='GS bucket_name where the input files are present')

    parser.add_argument(
        '--input-path',
        dest='input_path',
        required=False,
        help='GS folder name, if the input files are inside a bucket folder')

    # File name with the list of directories of data (one for each table)
    parser.add_argument(
        '--input-files-list',
        dest='input_files_list',
        required=True,
        help='File name of the files list')

    parser.add_argument('--bq-dataset',
                        dest='bq_dataset',
                        required=True,
                        default='rawdata',
                        help='Output BQ dataset to write the results to')

    # Parse arguments from the command line
    known_args, pipeline_args = parser.parse_known_args(argv)

    # Initiate the pipeline using the pipeline arguments
    logging.info('START - Pipeline')

    # DataIngestion is a class we built in this script to hold the logic for
    # transforming the file into a BigQuery table.
    data_ingestion = DataIngestion()

    # Get the list of folders (each with the report name) with the data and schema files
    input_files = get_file_gcs(known_args.input_bucket,
                               os.path.join(known_args.input_path, known_args.input_files_list)).decode()
    input_files = input_files.split("\n")

    # For each report generate a Dataflow job
    for input_file in input_files:
        p = beam.Pipeline(argv=pipeline_args)

        logging.info('START - Preparing file %s' % input_file)

        # Get the data file path
        data_path = os.path.join(
            'gs://' + known_args.input_bucket,
            known_args.input_path,
            input_file,
            'data.csv')

        table_name = input_file
        print(table_name)

        logging.info('Getting the schema from file')

        # Read the schema from the corresponding json file
        schema_encode = get_file_gcs(known_args.input_bucket,
                                     os.path.join(known_args.input_path, input_file, 'schema.json'))

        schema_json = json.loads(schema_encode)

        logging.info('GS path being read from: %s' % data_path)

        (p
         # Read the file. This is the source of the pipeline. All further
         # processing starts with lines read from the file. We use the input
         # argument from the command line. We also skip the first line which is a
         # header row.
         | 'Read from a File ' + input_file >> beam.io.ReadFromText(data_path, skip_header_lines=1)

         # This stage of the pipeline translates from a CSV file single row
         # input as a string, to a dictionary object consumable by BigQuery.
         # It refers to a function we have written. This function will
         # be run in parallel on different workers using input from the
         # previous stage of the pipeline.
         | 'String To BigQuery Row ' + input_file >>
         beam.Map(lambda s: data_ingestion.parse_method(string_input=s,schema_json=schema_json)) |

         # This stage of the pipeline translates from a CSV file single row
         # input as a string, to a dictionary object consumable by BigQuery.
         # It refers to a function we have written. This function will
         # be run in parallel on different workers using input from the
         # previous stage of the pipeline.
         # 'Inject Timestamp - ' + input_file >> beam.ParDo(InjectTimestamp()) |
         'Write to BigQuery - ' + input_file >> beam.io.WriteToBigQuery(
                    # The table name passed in from the command line
                    known_args.bq_dataset + '.' + table_name,
                    # Schema of the table
                    schema=schema_json,
                    # Creates the table in BigQuery if it does not exist
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    # Data will be appended to the table
                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
                )
         )

        logging.info('END - Preparing file %s' % input_file)

        p.run()
        logging.info('END - Pipeline')


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()