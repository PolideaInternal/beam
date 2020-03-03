/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.snowflake.test.unit;

import org.apache.beam.sdk.io.snowflake.data.text.SFBinary;
import org.apache.beam.sdk.io.snowflake.data.text.SFChar;
import org.apache.beam.sdk.io.snowflake.data.text.SFString;
import org.apache.beam.sdk.io.snowflake.data.text.SFText;
import org.apache.beam.sdk.io.snowflake.data.text.SFVarBinary;
import org.apache.beam.sdk.io.snowflake.data.text.SFVarchar;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class SFVarcharTest {
  @Test
  public void testSingleVarchar() {
    SFVarchar varchar = SFVarchar.of();

    assert varchar.sql().equals("VARCHAR");
  }

  @Test
  public void testSingleVarcharWithLimit() {
    SFVarchar varchar = SFVarchar.of(100);

    assert varchar.sql().equals("VARCHAR(100)");
  }

  @Test
  public void testString() {
    SFString str = SFString.of();

    assert str.sql().equals("VARCHAR");
  }

  @Test
  public void testText() {
    SFText text = SFText.of();

    assert text.sql().equals("VARCHAR");
  }

  @Test
  public void testBinary() {
    SFBinary binary = SFBinary.of();

    assert binary.sql().equals("BINARY");
  }

  @Test
  public void testVarBinary() {
    SFVarBinary binary = SFVarBinary.of();

    assert binary.sql().equals("BINARY");
  }

  @Test
  public void testBinaryWithLimit() {
    SFBinary binary = SFBinary.of(100);

    assert binary.sql().equals("BINARY(100)");
  }

  @Rule public ExpectedException exceptionRule = ExpectedException.none();

  @Test
  public void testBinaryReachesLimit() {
    exceptionRule.expect(IllegalArgumentException.class);
    SFBinary.of(8388609L);
  }

  @Test
  public void testChar() {
    SFChar sfChar = SFChar.of();

    assert sfChar.sql().equals("VARCHAR(1)");
  }
}
