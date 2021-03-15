package cn.edu.neu.wordcount;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * Desc
 * 需求:使用Flink完成WordCount-DataStream--使用lambda表达式
 * 编码步骤
 * 1.准备环境-env
 * 2.准备数据-source
 * 3.处理数据-transformation
 * 4.输出结果-sink
 * 5.触发执行-execute
 * @author 32098
 */
public class WordCount4Lambda {
    public static void main(String[] args) throws Exception {
        // 1.准备环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        //env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        //env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        // 2.准备数据-source
        DataStream<String> linesDS = env.fromElements("nhbd hadoop spark", "nhbd hadoop spark", "nhbd hadoop", "nhbd");

        // 3.处理数据-transformation

        // 3.1每一行数据按照空格切分成一个个的单词组成一个集合
        /*
        public interface FlatMapFunction<T, O> extends Function, Serializable {
            void flatMap(T value, Collector<O> out) throws Exception;
        }
         */
        // lambda表达式的语法:
        // (参数)->{方法体/函数体}
        // lambda表达式就是一个函数,函数的本质就是对象
        DataStream<String> wordsDS = linesDS.flatMap(
                (String value, Collector<String> out) -> Arrays.stream(value.split(" ")).forEach(out::collect)
        ).returns(Types.STRING);

        // 3.2对集合中的每个单词记为1
        /*
        public interface MapFunction<T, O> extends Function, Serializable {
            O map(T value) throws Exception;
        }
         */
        /*DataStream<Tuple2<String, Integer>> wordAndOnesDS = wordsDS.map(
                (String value) -> Tuple2.of(value, 1)
        ).returns(Types.TUPLE(Types.STRING, Types.INT));*/
        DataStream<Tuple2<String, Integer>> wordAndOnesDS = wordsDS.map(
                (String value) -> Tuple2.of(value, 1)
                , TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {})
        );

        // 3.3对数据按照单词(key)进行分组
        // 0表示按照tuple中的索引为0的字段,也就是key(单词)进行分组
        // KeyedStream<Tuple2<String, Integer>, Tuple> groupedDS = wordAndOnesDS.keyBy(0);
        // KeyedStream<Tuple2<String, Integer>, String> groupedDS = wordAndOnesDS.keyBy((KeySelector<Tuple2<String, Integer>, String>) t -> t.f0);
        KeyedStream<Tuple2<String, Integer>, String> groupedDS = wordAndOnesDS.keyBy(t -> t.f0);

        // 3.4对各个组内的数据按照数量(value)进行聚合就是求sum
        // 1表示按照tuple中的索引为1的字段也就是按照数量进行聚合累加!
        DataStream<Tuple2<String, Integer>> result = groupedDS.sum(1);

        // 4.输出结果-sink
        result.print();

        // 5.触发执行-execute
        env.execute();
    }
}

