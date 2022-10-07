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

package com.flinksql;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * Skeleton for a Flink DataStream Job.
 *
 * <p>For a tutorial how to write a Flink application, check the
 * tutorials and examples on the <a href="https://flink.apache.org">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class DataStreamJob {

	static class Book {
		public Book(Long id, String title, String authors, Integer year) {
			this.id = id;
			this.title = title;
			this.authors = authors;
			this.years = year;
		}
		final Long id;
		final String title;
		final String authors;
		final Integer years;
	}

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.fromElements(
				new Book(101L, "Stream Processing with Apache Flink", "Fabian Hueske, Vasiliki Kalavri", 2019),
				new Book(102L, "Streaming Systems", "Tyler Akidau, Slava Chernyak, Reuven Lax", 2018),
				new Book(103L, "Designing Data-Intensive Applications", "Martin Kleppmann", 2017),
				new Book(104L, "Kafka: The Definitive Guide", "Gwen Shapira, Neha Narkhede, Todd Palino", 2017)
		).addSink(
				JdbcSink.sink(
						"insert into books (id, title, authors, years) values (?, ?, ?, ?)",
						(statement, book) -> {
							statement.setLong(1, book.id);
							statement.setString(2, book.title);
							statement.setString(3, book.authors);
							statement.setInt(4, book.years);
						},
						JdbcExecutionOptions.builder()
								.withBatchSize(1000)
								.withBatchIntervalMs(200)
								.withMaxRetries(5)
								.build(),
						new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
								.withUrl("jdbc:mysql://127.0.0.1:3306/mydatabase?autoReconnect=true&useSSL=false")
								.withDriverName("com.mysql.jdbc.Driver")
								.withUsername("alagu")
								.withPassword("password")
								.build()
				));

		env.execute();
	}
}
