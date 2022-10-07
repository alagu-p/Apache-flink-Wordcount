package com.flinksql;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class Testcase {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings tableSettings = EnvironmentSettings.inBatchMode();

        StreamTableEnvironment tableEnv = StreamTableEnvironment
                .create(streamEnv, tableSettings);


//        tableEnv.sqlUpdate("CREATE TABLE ...");
//        Table table = tableEnv.sqlQuery("SELECT ... FROM ...");
//
//        DataStream<Row> stream = tableEnv.toAppendStream(table, Row.class);
//        stream.print();
//
//        tableEnv.execute("Print");
    }
}
