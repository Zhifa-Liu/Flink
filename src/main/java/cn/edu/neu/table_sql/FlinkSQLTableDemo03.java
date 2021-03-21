package cn.edu.neu.table_sql;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;


/**
 * @author 32098
 *
 * Desc requirement:
 * 使用 Table 方式对 DataStream 中的单词进行统计
 */
public class FlinkSQLTableDemo03 {
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class WordCount {
        public String word;
        public long frequency;
    }

    public static void main(String[] args) throws Exception {
        // 1.准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 2.Source
        DataStream<WordCount> input = env.fromElements(
                new WordCount("Hello", 1),
                new WordCount("World", 1),
                new WordCount("Hello", 1)
        );

        // 3.注册表
        Table table = tEnv.fromDataStream(input);

        // 4.执行查询
        Table resultTable = table
                .groupBy($("word"))
                .select($("word"), $("frequency").sum().as("frequency"))
                .filter(
                        $("frequency").isEqual(2)
                );

        // 5.输出结果
        DataStream<Tuple2<Boolean, WordCount >> resultDs = tEnv.toRetractStream(resultTable, WordCount.class);

        resultDs.print();

        env.execute();
    }
}


