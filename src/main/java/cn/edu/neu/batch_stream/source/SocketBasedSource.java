package cn.edu.neu.batch_stream.source;

import com.google.inject.internal.cglib.proxy.$FixedValue;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author 32098
 * master: nc -lk 9999
 */
public class SocketBasedSource {
    public static void main(String[] args) throws Exception {
        // 1. 准备环境
        // 1.1 创建流式执行环境实例对象
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        // 1.2 设置运行类型
        environment.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        // 2. 定义数据源
        DataStream<String> dsA = environment.socketTextStream("master", 9999);

        // 3. 处理数据
        // 3.1 sentence to word
        DataStream<String> dsB = dsA.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] words = s.split(" ");
                for (String word : words) {
                    collector.collect(word);
                }
            }
        });
        // 3.2 word to (word, 1)
        DataStream<Tuple2<String, Integer>> dsC = dsB.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return Tuple2.of(s, 1);
            }
        });
        // 3.3 (word, 1) to (word, word_count)
        DataStream<Tuple2<String, Integer>> result = dsC.keyBy(t -> t.f0).sum(1);

        // 4. sink
        result.print();

        // 5. execute
        environment.execute();
    }
}

