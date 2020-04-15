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

# pytype: skip-file

from __future__ import absolute_import

from apache_beam.transforms.external import ExternalTransform
from apache_beam.transforms.external import ImplicitSchemaPayloadBuilder


"""
  PTransforms for supporting Snowflake in Python pipelines. These transforms do not
  run a Snowflake client in Python. Instead, they expand to ExternalTransforms
  which the Expansion Service resolves to the Java SDK's SnowflakeIO. In other
  words: they are cross-language transforms.

  Note: To use this transform, you need to start the Java expansion service.
  Please refer to the portability documentation on how to do that. The
  expansion service address has to be provided when instantiating this
  transform. During pipeline translation this transform will be replaced by
  the Java SDK's GenerateSequence.

  If you start Flink's job server, the expansion service will be started on
  port 8097. This is also the configured default for this transform. For a
  different address, please set the expansion_service parameter.

  For more information see:
  - https://beam.apache.org/documentation/runners/flink/
  - https://beam.apache.org/roadmap/portability/

  Note: Runners need to support translating Read operations in order to use
  this source. At the moment only the Flink Runner supports this.

  Experimental; no backwards compatibility guarantees.
"""
class ReadFromSnowflake(ExternalTransform):
    URN = 'beam:external:java:snowflake:read:v1'

    def __init__(
            self,
            serverName,
            username,
            password,
            schema,
            database,
            stagingBucketName,
            storageIntegration,
            table=None,
            query=None,
            expansion_service=None):
        super(ReadFromSnowflake, self).__init__(
            self.URN,
            ImplicitSchemaPayloadBuilder({
                'serverName': serverName,
                'username': username,
                'password': password,
                'schema': schema,
                'database': database,
                'stagingBucketName': stagingBucketName,
                'storageIntegration': storageIntegration,
                'table': table,
                'query': query,
            }),
            expansion_service)

class WriteToSnowflake(ExternalTransform):

    URN = 'beam:external:java:snowflake:write:v1'

    def __init__(
            self,
            serverName,
            username,
            password,
            schema,
            database,
            stagingBucketName,
            storageIntegration,
            createDisposition,
            writeDisposition,
            parallelization = True,
            tableSchema= None,
            table=None,
            query=None,
            expansion_service=None):
        super(WriteToSnowflake, self).__init__(
            self.URN,
            ImplicitSchemaPayloadBuilder({
                'serverName': serverName,
                'username': username,
                'password': password,
                'schema': schema,
                'database': database,
                'stagingBucketName': stagingBucketName,
                'storageIntegration': storageIntegration,
                'createDisposition': createDisposition,
                'writeDisposition': writeDisposition,
                'parallelization': parallelization,
                'tableSchema': tableSchema,
                'table': table,
                'query': query,
            }),
            expansion_service)
