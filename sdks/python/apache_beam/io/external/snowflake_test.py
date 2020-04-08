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
import os
import re
import unittest

from nose.plugins.attrib import attr

from apache_beam.io.external.snowflake import ReadFromSnowflake
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

SERVER_NAME = "fy22127.eu-central-1.snowflakecomputing.com"
USERNAME = "uber"
PASSWORD = "qazpol123"
SCHEMA = "TPCH_SF1"
DATABASE = "SNOWFLAKE_SAMPLE_DATA"
STAGING_BUCKET_NAME = "darek-snowflake-tpch"
STORAGE_INTEGRATION = "DAREKTPCH"
TABLE = "LINEITEM"
EXPANSION_SERVICE = 'localhost:8097'

options = PipelineOptions([
  "--runner=FlinkRunner",
  "--flink_version=1.9",
  "--flink_master=localhost:8081",
  "--environment_type=LOOPBACK"
])

@attr('UsesCrossLanguageTransforms')
class XlangSnowflakeTest(unittest.TestCase):
  def test_snowflake_read(self):
    with TestPipeline(options=options) as p:
      res = (
          p
          | ReadFromSnowflake(SERVER_NAME, USERNAME, PASSWORD, SCHEMA, DATABASE, STAGING_BUCKET_NAME, STORAGE_INTEGRATION, TABLE, expansion_service=EXPANSION_SERVICE)
      )

      print(res)

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
