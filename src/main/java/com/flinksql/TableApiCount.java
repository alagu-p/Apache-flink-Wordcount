package com.flinksql;


import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.TextInputFormat;

import org.apache.flink.api.java.tuple.Tuple2;

import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;


public class TableApiCount {
    public static void main(String[] args) {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
            Path pa = new Path("/home/alaghu/flink-1.15.2/");


            TextInputFormat format = new TextInputFormat(pa);
            BasicTypeInfo typeInfo = BasicTypeInfo.STRING_TYPE_INFO;

            DataStream<String> st = env.readFile(format, "/home/alaghu/InDir/", FileProcessingMode.PROCESS_CONTINUOUSLY, 1L, (TypeInformation) typeInfo);

            DataStream<Tuple2<String, Integer>> counts =
                    // split up the lines in pairs (2-tuples) containing: (word,1)
                    st.flatMap(new WordcountAndStore.Tokenizer())
                            // group by the tuple field "0" and sum up tuple field "1"
                            .keyBy(value -> value.f0)
                            .sum(1).name("counter");




// register the DataStream as view "MyView" in the current session
// all columns are derived automatically
            //tableEnv.fromDataStream(counts);
            tableEnv.createTemporaryView("wordc", counts);

        tableEnv.executeSql("create table wor(word STRING,wcount INT) WITH(" +
                "'connector' = 'jdbc'," +
                " 'url' = 'jdbc:mysql://localhost:3306/mydatabase?autoReconnect=true&useSSL=false'," +
                "  'table-name' = 'wordcount'," +
                    "'driver'     = 'com.mysql.jdbc.Driver'," +
                    "'username'='alagu'," +
                    "'password'='password'," +
                    "'sink.buffer-flush.max-rows' = '10'," +
                    "'sink.buffer-flush.interval' = '1s')");
            //Table Trans=tableEnv.from("wordcount");
            //tableEnv.from("MyView").printSchema();
        //tableEnv.fromDataStream(counts);
//        tableEnv.executeSql("Select * from wor").print();
            //tableEnv.executeSql("Select * from wor").print();
        //tableEnv.executeSql("Select * from wordc").print();
            tableEnv.executeSql("insert into  wor select f0,f1 from wordc");
            //DataStream<Row> stream = tableEnv.toAppendStream(table, Row.class);
        //tableEnv.executeSql("Select * from wor").print();
       // stream.print();
        tableEnv.executeSql("Select * from wor").print();
        }
        public static final class Tokenizer
                implements FlatMapFunction<String, Tuple2<String, Integer>> {

            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
                // normalize and split the line
                String[] tokens = value.toLowerCase().split("\\W+");

                // emit the pairs
                for (String token : tokens) {
                    if (token.length() > 0) {
                        out.collect(new Tuple2<>(token, 1));
                    }
                }
            }
        }

    }

