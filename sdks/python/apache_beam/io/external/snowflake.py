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

import typing

from past.builtins import unicode

import apache_beam as beam
from apache_beam.transforms.external import ExternalTransform
from apache_beam.transforms.external import ImplicitSchemaPayloadBuilder
from apache_beam.transforms.external import NamedTupleBasedPayloadBuilder

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

ReadFromSnowflakeSchema = typing.NamedTuple(
    'WriteToSnowflakeSchema',
    [
        ('server_name', unicode),
        ('schema', unicode),
        ('database', unicode),
        ('staging_bucket_name', unicode),
        ('storage_integration', unicode),
        ('username', typing.Optional[unicode]),
        ('password', typing.Optional[unicode]),
        ('private_key_file', typing.Optional[unicode]),
        ('private_key_password', typing.Optional[unicode]),
        ('o_auth_token', typing.Optional[unicode]),
        ('table', typing.Optional[unicode]),
        ('query', typing.Optional[unicode]),
    ])


class ReadFromSnowflake(beam.PTransform):
  """An external PTransform which reads from Snowflake."""

  URN = 'beam:external:java:snowflake:read:v1'

  def __init__(
      self,
      server_name,
      schema,
      database,
      staging_bucket_name,
      storage_integration,
      csv_mapper,
      username=None,
      password=None,
      private_key_file=None,
      private_key_password=None,
      o_auth_token=None,
      table=None,
      query=None,
      expansion_service=None):

    self.params = ReadFromSnowflakeSchema(
        server_name=server_name,
        schema=schema,
        database=database,
        staging_bucket_name=staging_bucket_name,
        storage_integration=storage_integration,
        username=username,
        password=password,
        private_key_file=private_key_file,
        private_key_password=private_key_password,
        o_auth_token=o_auth_token,
        table=table,
        query=query,
    )
    self.csv_mapper = csv_mapper
    self.expansion_service = expansion_service

  def expand(self, pbegin):
    return (
        pbegin
        | ExternalTransform(
            self.URN,
            NamedTupleBasedPayloadBuilder(self.params),
            self.expansion_service,
        )
        | 'csv_to_array_mapper' >> beam.Map(lambda csv: csv.split(b','))
        | 'csv_mapper' >> beam.Map(self.csv_mapper))


WriteToSnowflakeSchema = typing.NamedTuple(
    'WriteToSnowflakeSchema',
    [
        ('server_name', unicode),
        ('schema', unicode),
        ('database', unicode),
        ('staging_bucket_name', unicode),
        ('storage_integration', unicode),
        ('create_disposition', unicode),
        ('write_disposition', unicode),
        ('table_schema', unicode),
        ('username', typing.Optional[unicode]),
        ('password', typing.Optional[unicode]),
        ('private_key_file', typing.Optional[unicode]),
        ('private_key_password', typing.Optional[unicode]),
        ('o_auth_token', typing.Optional[unicode]),
        ('table', typing.Optional[unicode]),
        ('query', typing.Optional[unicode]),
    ])


class WriteToSnowflake(beam.PTransform):
  """An external PTransform which writes to Snowflake."""

  URN = 'beam:external:java:snowflake:write:v1'

  def __init__(
      self,
      server_name,
      schema,
      database,
      staging_bucket_name,
      storage_integration,
      create_disposition,
      write_disposition,
      table_schema,
      user_data_mapper,
      username=None,
      password=None,
      private_key_file=None,
      private_key_password=None,
      o_auth_token=None,
      table=None,
      query=None,
      expansion_service=None):

    self.params = WriteToSnowflakeSchema(
        server_name=server_name,
        schema=schema,
        database=database,
        staging_bucket_name=staging_bucket_name,
        storage_integration=storage_integration,
        create_disposition=create_disposition,
        write_disposition=write_disposition,
        table_schema=table_schema,
        username=username,
        password=password,
        private_key_file=private_key_file,
        private_key_password=private_key_password,
        o_auth_token=o_auth_token,
        table=table,
        query=query,
    )
    self.user_data_mapper = user_data_mapper
    self.expansion_service = expansion_service

  def expand(self, pbegin):
    return (
        pbegin
        | 'user_data_mapper' >> beam.Map(
            self.user_data_mapper).with_output_types(typing.List[bytes])
        | ExternalTransform(
            self.URN,
            NamedTupleBasedPayloadBuilder(self.params),
            self.expansion_service,
        ).with_output_types(typing.Any))
