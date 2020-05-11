#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
Unit tests for cross-language snowflake io operations.

To run this test you need to perform this script first:
set -xe
./gradlew -p sdks/java/io/snowflake spotlessApply build
./gradlew -p sdks/java docker
docker tag apache/beam_java_sdk:2.21.0.dev apache/beam_java_sdk:2.20.0.dev #hack for X-lang that on 2.21 branch 2.20 is still used
./gradlew -p runners/flink/1.10/job-server shadowJar
./gradlew -p runners/flink/1.10/job-server run

Example of run:

python setup.py nosetests --tests=apache_beam.io.external.snowflake_test --test-pipeline-options="
  --server_name=<SNOWFLAKE_SERVER_NAME>
  --username=<SNOWFLAKE_USERNAME>
  --password=<SNOWFLAKE_PASSWORD>
  --private_key_file=<PATH_TO_PRIVATE_KEY_FILE>
  --private_key_passphrase=<PASSWORD_TO_PRIVATE_KEY>
  --oauth_token=<TOKEN>
  --staging_bucket_name=<GCP_BUCKET_NAME_NOT_PATH>
  --storage_integration=<SNOWFLAKE_STORAGE_INTEGRATION_NAME>
  --database=<DATABASE>
  --schema=<SCHEMA>
  --table=<TABLE_NAME>
  --expansion_service=localhost:8097
  --runner=FlinkRunner
  --flink_version=1.10
  --flink_master=localhost:8081
  --environment_type=LOOPBACK"

"""

# pytype: skip-file

import argparse
import logging
import unittest

from apache_beam.io.gcp.gcsfilesystem import GCSFileSystem
from nose.plugins.attrib import attr

import apache_beam as beam
from apache_beam.io.external.generate_sequence import GenerateSequence
from apache_beam.io.external.snowflake import ReadFromSnowflake, WriteToSnowflake
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

SCHEMA_STRING = """
{"schema":[
    {"dataType":{"type":"text","length":null},"name":"text_column","nullable":true},
    {"dataType":{"type":"integer","precision":38,"scale":0},"name":"number_column","nullable":false},
    {"dataType":{"type":"boolean"},"name":"boolean_column","nullable":false}
]}
"""

class TestRow(object):
  def __init__(self, text_column, number_column, boolean_column):
    self.text_column = text_column
    self.number_column = number_column
    self.boolean_column = boolean_column

  def __eq__(self, other):
    return self.text_column == other.text_column and \
           self.number_column == other.number_column and \
           self.boolean_column == other.boolean_column


@attr('UsesCrossLanguageTransforms')
class SnowflakeTest(unittest.TestCase):
  def test_snowflake_write_read(self):
    self.run_write()
    self.run_read()
    self.clean_up()

  def run_write(self):
    def user_data_mapper(test_row):
      return [
          test_row.text_column.encode('utf-8'),
          str(test_row.number_column).encode('utf-8'),
          str(test_row.boolean_column).encode('utf-8'),
      ]

    with TestPipeline(options=PipelineOptions(self.pipeline_args)) as p:
      # TODO make it work with beam.Create([TestRow())
      (
          p
          | GenerateSequence(
              start=1, stop=3, expansion_service=self.expansion_service)
          | beam.Map(lambda num: TestRow("test" + str(num), num, True))
          | WriteToSnowflake(
              server_name=self.server_name,
              username=self.username,
              password=self.password,
              o_auth_token=self.o_auth_token,
              private_key_file=self.private_key_file,
              private_key_password=self.private_key_password,
              schema=self.schema,
              database=self.database,
              staging_bucket_name=self.staging_bucket_name,
              storage_integration=self.storage_integration,
              create_disposition="CREATE_IF_NEEDED",
              write_disposition="TRUNCATE",
              table_schema=SCHEMA_STRING,
              user_data_mapper=user_data_mapper,
              table=self.table,
              query=None,
              expansion_service=self.expansion_service,
          ))

  def run_read(self):
    def csv_mapper(strings_array):
      return TestRow(
          strings_array[0], int(strings_array[1]), bool(strings_array[2]))

    with TestPipeline(options=PipelineOptions(self.pipeline_args)) as p:
      result = (
          p
          | ReadFromSnowflake(
              server_name=self.server_name,
              username=self.username,
              password=self.password,
              o_auth_token=self.o_auth_token,
              private_key_file=self.private_key_file,
              private_key_password=self.private_key_password,
              schema=self.schema,
              database=self.database,
              staging_bucket_name=self.staging_bucket_name,
              storage_integration=self.storage_integration,
              csv_mapper=csv_mapper,
              table=self.table,
              query=None,
              expansion_service=self.expansion_service))

      assert_that(
          result,
          equal_to([TestRow(b'test1', 1, True), TestRow(b'test2', 2, True)]))

  def clean_up(self):
    GCSFileSystem(pipeline_options=PipelineOptions()) \
        .delete([GCSFileSystem.GCS_PREFIX + self.staging_bucket_name + '/data/*'])

  def setUp(self):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--server_name',
        required=True,
        help=(
            'Snowflake server name of the form '
            '<SNOWFLAKE_ACCOUNT_NAME>.snowflakecomputing.com'),
    )
    parser.add_argument(
        '--username',
        help='Snowflake username',
    )
    parser.add_argument(
        '--password',
        help='Snowflake password',
    )
    parser.add_argument(
        '--private_key_file',
        help='Private key file path',
    )
    parser.add_argument(
        '--private_key_password',
        help='Password to private key',
    )
    parser.add_argument(
        '--o_auth_token',
        help='OAuth token',
    )
    parser.add_argument(
        '--staging_bucket_name',
        required=True,
        help='GCP staging bucket name (not path)',
    )
    parser.add_argument(
        '--storage_integration',
        required=True,
        help='Snowflake integration name',
    )
    parser.add_argument(
        '--database',
        required=True,
        help='Snowflake database name',
    )
    parser.add_argument(
        '--schema',
        required=True,
        help='Snowflake schema name',
    )
    parser.add_argument(
        '--table',
        required=True,
        help='Snowflake table name',
    )
    parser.add_argument(
        '--expansion_service',
        default='localhost:8097',
    )

    pipeline = TestPipeline()
    argv = pipeline.get_full_options_as_args()

    known_args, self.pipeline_args = parser.parse_known_args(argv)

    self.server_name = known_args.server_name
    self.database = known_args.database
    self.schema = known_args.schema
    self.table = known_args.table
    self.username = known_args.username
    self.password = known_args.password
    self.private_key_file = known_args.private_key_file
    self.private_key_password = known_args.private_key_password
    self.o_auth_token = known_args.o_auth_token
    self.staging_bucket_name = known_args.staging_bucket_name
    self.storage_integration = known_args.storage_integration
    self.expansion_service = known_args.expansion_service

    self.assertTrue(
        self.o_auth_token or (self.username and self.password) or
        (self.username and self.private_key_file and self.private_key_password),
        'No credentials given',
    )


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
