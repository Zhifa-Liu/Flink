package cn.edu.neu.batch_stream.transformation;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author 32098
 * map
 * flatMap
 * keyBy
 * filter
 * sum
 * reduce
 *
 * Desc requirement
 * 对流数据中的单词进行统计，排除敏感词godie
 */
public class BaseTransformation {
    public static void main(String[] args) {
        // 1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        // 2.source
        DataStream<String> linesDs = env.socketTextStream("master", 9999);

        // 3.处理数据: transformation
        DataStream<String> wordsDs = linesDs.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                // value 为一行行的数据
                String[] words = value.split(" ");
                for (String word : words) {
                    // 将切割处理的一个个的单词收集起来并返回
                    out.collect(word);
                }
            }
        });
        DataStream<String> filtedDs = wordsDs.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                return !"godie".equals(s);
            }
        });
        DataStream<Tuple2<String, Integer>> wordAndOnesDs = filtedDs.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                //value就是进来一个个的单词
                return Tuple2.of(value, 1);
            }
        });
        KeyedStream<Tuple2<String, Integer>, String> groupedDs = wordAndOnesDs.keyBy(t -> t.f0);

        DataStream<Tuple2<String, Integer>> resultA = groupedDs.sum(1);
        DataStream<Tuple2<String, Integer>> resultB = groupedDs.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                return Tuple2.of(value1.f0, value1.f1 + value1.f1);
            }
        });

        //4.sink
        resultA.print("A");
        resultB.print("B");

        //5.触发执行-execute
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
