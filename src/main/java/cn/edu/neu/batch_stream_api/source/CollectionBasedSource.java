package cn.edu.neu.batch_stream_api.source;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @author 32098
 * 1.env.fromElements(可变参数);
 * 2.env.fromColletion(各种集合);
 * 3.env.generateSequence(开始,结束);
 * 4.env.fromSequence(开始,结束);
 */
public class CollectionBasedSource {
    public static void main(String[] args) {
        // 1. 准备环境
        // 1.1 创建流式执行环境实例对象
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        // 1.2 设置运行类型
        environment.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        // 2. 定义数据源
        DataStream<String> dataStreamA = environment.fromElements("hadoop", "spark", "storm", "flink");
        DataStream<String> dataStreamB = environment.fromCollection(Arrays.asList("hadoop", "spark", "storm", "flink"));
        DataStream<Long> dataStreamC = environment.generateSequence(1, 10);
        DataStream<Long> dataStreamD = environment.fromSequence(1, 10);

        // 3. Transformation: 略

        // 4. Sink
        dataStreamA.print("A");
        dataStreamB.print("B");
        dataStreamC.print("C");
        dataStreamD.print("D");

        try {
            environment.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
