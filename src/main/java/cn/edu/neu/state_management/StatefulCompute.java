package cn.edu.neu.state_management;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author 32098
 *
 * Flink 中已经对需要进行有状态计算的API，做了封装，底层已经维护好了状态!
 */
public class StatefulCompute {
    public static void main(String[] args) throws Exception {
        // 1.获取环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        // 2.定义来源-source
        DataStream<String> linesDS = env.socketTextStream("master", 9999);

        // 3.处理数据-transformation
        // 3.1 每一行数据按照空格切分成一个个的单词组成一个集合
        DataStream<String> wordsDS = linesDS.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                //value就是一行行的数据
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(word);//将切割处理的一个个的单词收集起来并返回
                }
            }
        });
        // 3.2 对集合中的每个单词记为1
        DataStream<Tuple2<String, Integer>> wordAndOnesDS = wordsDS.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                //value就是进来一个个的单词
                return Tuple2.of(value, 1);
            }
        });

        // 3.3对数据按照单词(key)进行分组
        KeyedStream<Tuple2<String, Integer>, String> groupedDS = wordAndOnesDS.keyBy(t -> t.f0);
        // 3.4对各个组内的数据按照数量(value)进行聚合就是求sum
        DataStream<Tuple2<String, Integer>> result = groupedDS.sum(1);

        // 4.输出结果-sink
        result.print();

        // 5.触发执行-execute
        env.execute();
    }
}

/*
执行 netcat (nc -lk 9999)，然后在终端输入 hello world，执行程序会输出什么?
# 答案很明显，(hello, 1)和 (word,1)
那么问题来了，如果再次在终端输入 hello world，程序会输入什么?
# 答案其实也很明显，(hello, 2)和(world, 2)。
 */


