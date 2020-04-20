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
    {"dataType":{"type":"text","length":null},"name":"name","nullable":true},
    {"dataType":{"type":"text","length":null},"name":"description","nullable":false}
]}
"""

OPTIONS = PipelineOptions([
    "--runner=FlinkRunner",
    "--flink_version=1.10",
    "--flink_master=localhost:8081",
    "--environment_type=LOOPBACK"
])


@attr('UsesCrossLanguageTransforms')
class XlangSnowflakeTest(unittest.TestCase):

    def test_snowflake_write(self):
        # TODO For now is possible only to run write or read due to memory leak
        write_result = run_write()
        # read_result = run_read()


def run_write():
    with TestPipeline(options=OPTIONS, blocking=True) as p:
        return (p
                | GenerateSequence(start=1, stop=2, expansion_service=EXPANSION_SERVICE)
                | beam.Map(lambda num: ["test ", "test test"])
                | WriteToSnowflake(serverName=SERVER_NAME,
                                   username=USERNAME,
                                   password=PASSWORD,
                                   schema=SCHEMA,
                                   database=DATABASE,
                                   stagingBucketName=STAGING_BUCKET_NAME,
                                   storageIntegration=STORAGE_INTEGRATION,
                                   createDisposition="CREATE_IF_NEEDED",
                                   writeDisposition="TRUNCATE",
                                   parallelization=False,
                                   tableSchema=SCHEMA_STRING,
                                   table=TABLE,
                                   query=None,
                                   expansion_service=EXPANSION_SERVICE
                                   )
                )


def run_read():
    with TestPipeline(options=OPTIONS, blocking=True) as p:
        def print_fn(message):
            print('VALUE: {}'.format(message))
            return message

        return (p
                | ReadFromSnowflake(serverName=SERVER_NAME,
                                    username=USERNAME,
                                    password=PASSWORD,
                                    schema=SCHEMA,
                                    database=DATABASE,
                                    stagingBucketName=STAGING_BUCKET_NAME,
                                    storageIntegration=STORAGE_INTEGRATION,
                                    table=TABLE,
                                    query=None,
                                    expansion_service=EXPANSION_SERVICE
                                    )
                | beam.Map(print_fn)
                )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    unittest.main()
