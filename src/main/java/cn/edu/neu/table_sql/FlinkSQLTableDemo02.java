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
 * 使用 SQL 方式对 DataStream 中的单词进行统计
 */
public class FlinkSQLTableDemo02 {
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

        // 2.source
        DataStream<WordCount> input = env.fromElements(
                new WordCount("Hello", 1),
                new WordCount("World", 1),
                new WordCount("Hello", 1)
        );

        // 3.注册表
        tEnv.createTemporaryView("WordCount", input, $("word"), $("frequency"));

        // 4.执行查询
        Table resultTable = tEnv.sqlQuery("SELECT word, SUM(frequency) as frequency FROM WordCount GROUP BY word");

        // 5.输出结果
        // DELETE+INSERT=UPDATE
        DataStream<Tuple2<Boolean, WordCount>> resultDs = tEnv.toRetractStream(resultTable, WordCount.class);
        /*
        Can't
        tEnv.toAppendStream doesn't support consuming update changes which is produced by node GroupAggregate
        DataStream<WC> resultDS = tEnv.toAppendStream(resultTable, WordCount.class);
         */

        // 6.sink
        resultDs.print();

        // 7.execute
        env.execute();
    }
}

