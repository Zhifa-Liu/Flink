package cn.edu.neu.wordcount;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Desc
 * 需求:使用Flink完成WordCount-DataStream
 * 编码步骤
 * 1.准备环境-env
 * 2.准备数据-source
 * 3.处理数据-transformation
 * 4.输出结果-sink
 * 5.触发执行-execute
 * @author 32098
 */
public class WordCount4DataStream {
    public static void main(String[] args) throws Exception {
        // 新版本的流批统一API，既支持流处理也支持批处理
        // 1.准备环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        // env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        // env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        // 2.准备数据-source
        DataStream<String> linesDS = env.fromElements("nhbd hadoop spark","nhbd hadoop spark","nhbd hadoop","nhbd");

        // 3.处理数据-transformation

        // 3.1每一行数据按照空格切分成一个个的单词组成一个集合
        /*
        public interface FlatMapFunction<T, O> extends Function, Serializable {
            void flatMap(T value, Collector<O> out) throws Exception;
        }
         */
        DataStream<String> wordsDS = linesDS.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                // value就是一行行的数据
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(word); // 将切割处理的一个个的单词收集起来并返回
                }
            }
        });

        // 3.2对集合中的每个单词记为1
        /*
        public interface MapFunction<T, O> extends Function, Serializable {
            O map(T value) throws Exception;
        }
         */
        DataStream<Tuple2<String, Integer>> wordAndOnesDS = wordsDS.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                // value就是进来一个个的单词
                return Tuple2.of(value, 1);
            }
        });

        // 3.3对数据按照单词(key)进行分组
        // 0表示按照tuple中的索引为0的字段,也就是key(单词)进行分组
        // KeyedStream<Tuple2<String, Integer>, Tuple> groupedDS = wordAndOnesDS.keyBy(0);
        KeyedStream<Tuple2<String, Integer>, String> groupedDS = wordAndOnesDS.keyBy(t -> t.f0);


        // 3.4对各个组内的数据按照数量(value)进行聚合就是求sum
        // 1表示按照tuple中的索引为1的字段也就是按照数量进行聚合累加!
        DataStream<Tuple2<String, Integer>> result = groupedDS.sum(1);

        // 4.输出结果-sink
        result.print();

        // 5.触发执行-execute
        env.execute(); //DataStream需要调用execute
    }
}
