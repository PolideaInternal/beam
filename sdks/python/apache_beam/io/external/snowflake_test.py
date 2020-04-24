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

"""Unit tests for cross-language generate sequence."""

# pytype: skip-file

from __future__ import absolute_import
from __future__ import print_function

import logging
import unittest

from nose.plugins.attrib import attr
import apache_beam as beam
from apache_beam.io.external.snowflake import ReadFromSnowflake, WriteToSnowflake
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.external.generate_sequence import GenerateSequence
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

SERVER_NAME = ""
USERNAME = ""
PASSWORD = ""
SCHEMA = ""
DATABASE = ""
STAGING_BUCKET_NAME = ""
STORAGE_INTEGRATION = ""
TABLE = ""
EXPANSION_SERVICE = 'localhost:8097'
SCHEMA_STRING = """
{"schema":[
    {"dataType":{"type":"text","length":null},"name":"text_column","nullable":true},
    {"dataType":{"type":"integer","precision":38,"scale":0},"name":"number_column","nullable":false},
    {"dataType":{"type":"boolean"},"name":"boolean_column","nullable":false}
]}
"""

OPTIONS = [
    "--runner=FlinkRunner",
    "--flink_version=1.10",
    "--flink_master=localhost:8081",
    "--environment_type=LOOPBACK"
]

class TestRow(object):
    def __init__(self, text_column, number_column, boolean_column):
        self.text_column = text_column
        self.number_column = number_column
        self.boolean_column = boolean_column

    def __eq__(self, other):
        return self.text_column == other.text_column and self.number_column == other.number_column and self.boolean_column == other.boolean_column


@attr('UsesCrossLanguageTransforms')
class SnowflakeTest(unittest.TestCase):

    def test_snowflake_write_read(self):
        run_write()
        run_read()

def run_write():

    def user_data_mapper(test_row):
        return [test_row.text_column, str(test_row.number_column), str(test_row.boolean_column)]

    with TestPipeline(options=PipelineOptions(OPTIONS)) as p:
        # TODO make it work with beam.Create([TestRow())
        p \
        | GenerateSequence(start=1, stop=3, expansion_service=EXPANSION_SERVICE) \
        | beam.Map(lambda num: TestRow("test" + str(num) , num, True)) \
        | WriteToSnowflake(server_name=SERVER_NAME,
                           username=USERNAME,
                           password=PASSWORD,
                           schema=SCHEMA,
                           database=DATABASE,
                           staging_bucket_name=STAGING_BUCKET_NAME,
                           storage_integration=STORAGE_INTEGRATION,
                           create_disposition="CREATE_IF_NEEDED",
                           write_disposition="TRUNCATE",
                           table_schema=SCHEMA_STRING,
                           user_data_mapper= user_data_mapper,
                           parallelization=False,
                           table=TABLE,
                           query=None,
                           expansion_service=EXPANSION_SERVICE
                           )

def run_read():
    def csv_mapper(strings_array):
        return TestRow(strings_array[0], int(strings_array[1]), bool(strings_array[2]))

    with TestPipeline(options=PipelineOptions(OPTIONS)) as p:
        result = p \
                 | ReadFromSnowflake(server_name=SERVER_NAME,
                                     username=USERNAME,
                                     password=PASSWORD,
                                     schema=SCHEMA,
                                     database=DATABASE,
                                     staging_bucket_name=STAGING_BUCKET_NAME,
                                     storage_integration=STORAGE_INTEGRATION,
                                     csv_mapper=csv_mapper,
                                     table=TABLE,
                                     query=None,
                                     expansion_service=EXPANSION_SERVICE
                                     )

        assert_that(result, equal_to([TestRow("test1",1, True),TestRow("test2",2, True)]))

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    unittest.main()
