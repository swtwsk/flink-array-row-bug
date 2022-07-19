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

package com.github.swtwsk;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkArrayRowBugJob {
	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

		final Path currentPath = Paths.get("");

		final String createTable = String.format(
				"CREATE TABLE wrongArray (\n" +
				"    foo bigint,\n" +
				"    bar ARRAY<ROW<`foo1` STRING, `foo2` STRING>>,\n" +
				"    strings ARRAY<STRING>,\n" +
				"    intRows ARRAY<ROW<`a` INT, `b` INT>>\n" +
				") WITH (\n" +
				"  'connector' = 'filesystem',\n" +
				"  'path' = '%s',\n" +
				"  'format' = 'json'\n" +
				")",
				"file://" + currentPath.toAbsolutePath());
		tEnv.executeSql(createTable);

		final String insert =
				"insert into wrongArray (\n" +
				"    SELECT\n" +
				"        1,\n" +
				"        array[\n" +
				"            ('Field1', 'Value1'),\n" +
				"            ('Field2', 'Value2')\n" +
				"        ],\n" +
				"        array['foo', 'bar', 'foobar'],\n" +
				"        array[ROW(1, 1), ROW(2, 2)]\n" +
				"    FROM (VALUES(1))\n" +
				")";

		tEnv.executeSql(insert).await();
	}
}
