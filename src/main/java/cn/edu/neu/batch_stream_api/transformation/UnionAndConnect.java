package cn.edu.neu.batch_stream_api.transformation;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * @author 32098
 *
 * union: 合并多个（more than two）同类型的数据流，并生成同类型的数据流; FIFO; With Duplicate
 * connect: 合并两个数据流（类型可不一致） ===> ConnectedStreams (no print())
 *
 * Desc requirement:
 * 1. union two string stream
 * 2. connect a string stream and a int stream
 */
public class UnionAndConnect {
    public static void main(String[] args) {
        // 1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        // 2.Source
        DataStream<String> dsA = env.fromElements("hadoop", "spark", "storm");
        DataStream<String> dsB = env.fromElements("hadoop", "spark", "flink");
        DataStream<Integer> dsC = env.fromElements(1, 2, 3);

        // 3.transformation
        DataStream<String> unionStream = dsA.union(dsB);
        ConnectedStreams<String, Integer> connectStream = dsA.connect(dsC);

        DataStream<String> dealConnectStream = connectStream.map(new CoMapFunction<String, Integer, String>() {
            @Override
            public String map1(String s) throws Exception {
                return "String to String:" + s;
            }

            @Override
            public String map2(Integer integer) throws Exception {
                return "Integer to String" + integer.toString();
            }
        });

        // 4.sink
        unionStream.print();
        dealConnectStream.print();

        // 5.execute
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}


