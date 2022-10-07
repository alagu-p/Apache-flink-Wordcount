package com.flinksql;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.IOException;

import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.FileUtils;
import scala.Int;

import java.io.File;
import java.io.IOException;
public class WCFlinkToMysql {
    public static void main(String[] args) throws Exception {

       final String word;
        final Int wcount;
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Path pa = new Path("/home/alaghu/flink-1.15.2/README.txt");


        TextInputFormat format = new TextInputFormat(pa);
        BasicTypeInfo typeInfo = BasicTypeInfo.STRING_TYPE_INFO;
//        DataStream<Tuple2<Long, String>> dataStream = env.fromElements(
//                Tuple2.of(12L, "Alice"),
//                Tuple2.of(0L, "Bob"));
        DataStream<String> st = env.readFile(format, "/home/alaghu/InDir/", FileProcessingMode.PROCESS_CONTINUOUSLY, 1L, (TypeInformation) typeInfo);

        DataStream<Tuple2<String, Integer>> counts =
                // split up the lines in pairs (2-tuples) containing: (word,1)
                st.flatMap(new WordcountAndStore.Tokenizer())
                        // group by the tuple field "0" and sum up tuple field "1"
                        .keyBy(value -> value.f0)
                        .sum(1).name("counts");

        Table inputTable = tableEnv.fromDataStream(counts).as("Word", "counts");

// register the DataStream as view "MyView" in the current session
// all columns are derived automatically

        tableEnv.createTemporaryView("MyView", inputTable);
        tableEnv.from("MyView").printSchema();
//        Table resultTable = tableEnv.sqlQuery("Select * from MyView group by word");
        tableEnv.executeSql("Select * from MyView").print();
//        DataStream<Row> resultStream = tableEnv.toDataStream(resultTable);
//        resultStream.print();
        env.execute();

        env.fromElements(inputTable).addSink(
                JdbcSink.sink(
                        "insert into wordcount (word,wcount) values (?, ?)",
                        (statement, table) -> {
//                            statement.setLong(1, word);
//                            statement.setString(2, wcount);

                        },
                        JdbcExecutionOptions.builder()
                                .withBatchSize(1000)
                                .withBatchIntervalMs(200)
                                .withMaxRetries(5)
                                .build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl("jdbc:mysql://127.0.0.1:3306/mydatabase")
                                .withDriverName("com.mysql.jdbc.Driver")
                                .withUsername("alagu")
                                .withPassword("password")
                                .build()
                ));
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
