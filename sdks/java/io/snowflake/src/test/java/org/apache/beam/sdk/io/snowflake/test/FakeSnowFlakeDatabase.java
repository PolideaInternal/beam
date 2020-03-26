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
package org.apache.beam.sdk.io.snowflake.test;

import java.util.*;

/**
 * Stores tables and their contents in memory.
 */
public class FakeSnowFlakeDatabase {
    public static final String FAKE_TABLE = "FAKE_TABLE";

    private static FakeSnowFlakeDatabase instance = null;

    private Map<String, List<String>> tables;

    private FakeSnowFlakeDatabase() {
        tables = new HashMap<>();

        List<String> rows =
                Arrays.asList(
                        "3628897,108036,8037,1,8.00,8352.24,0.04,0.00,'N','O','1996-09-16','1996-07-26','1996-10-01','TAKE BACK RETURN','TRUCK','uctions play car'",
                        "3628897,145958,5959,2,19.00,38075.05,0.05,0.03,'N','O','1996-10-05','1996-08-10','1996-10-26','DELIVER IN PERSON','MAIL','ges boost. pending instruction',",
                        "3628897,105704,5705,3,32.00,54710.40,0.10,0.03,'N','O','1996-06-30','1996-08-25','1996-07-06','TAKE BACK RETURN','FOB',' carefully'",
                        "3628900,119015,1527,1,11.00,11374.11,0.02,0.01,'N','O','1998-11-16','1998-09-07','1998-12-10','DELIVER IN PERSON','RAIL','arefully bold attainments haggle f'",
                        "3628900,76282,3804,2,49.00,61655.72,0.02,0.05,'N','O','1998-10-11','1998-09-26','1998-11-09','TAKE BACK RETURN','RAIL',' above the bold requests. regu'",
                        "3628900,57758,5274,3,33.00,56619.75,0.03,0.07,'N','O','1998-08-06','1998-10-13','1998-08-29','NONE','AIR','yly bold instruct'");

        tables.put(FAKE_TABLE, rows);
    }

    public static FakeSnowFlakeDatabase getInstance() {
        if (instance == null) instance = new FakeSnowFlakeDatabase();
        return instance;
    }

    public FakeSnowFlakeDatabase(Map<String, List<String>> tables) {
        this.tables = new HashMap<>();
    }

    public Map<String, List<String>> getTables() {
        return tables;
    }

    public List<String> getTable(String table) {
        return this.tables.get(table);
    }

    public List<String> putTable(String table, List<String> rows) {
        return this.tables.put(table, rows);
    }
}
