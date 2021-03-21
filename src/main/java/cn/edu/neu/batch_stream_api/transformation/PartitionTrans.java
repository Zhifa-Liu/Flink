package cn.edu.neu.batch_stream_api.transformation;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author 32098
 *
 * rebalance partitioin: 解决分区数据倾斜问题
 */
public class PartitionTrans {
    public static void main(String[] args) throws Exception {
        // 1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC).setParallelism(3);

        // 2.source
        DataStream<Long> longDs = env.fromSequence(0, 1000);

        // 3.transformation
        // 随机分配，to get 数据倾斜
        DataStream<Long> filterDs = longDs.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long num) throws Exception {
                return num > 100;
            }
        });

        // integer => (partition(subtask)_id，integer_count)
        DataStream<Tuple2<Integer, Integer>> a = filterDs.map(
            new RichMapFunction<Long, Tuple2<Integer, Integer>>() {
                @Override
                public Tuple2<Integer, Integer> map(Long aLong) throws Exception {
                    int id = getRuntimeContext().getIndexOfThisSubtask();
                    return Tuple2.of(id, 1);
                }
            }
        ).keyBy(t -> t.f0).sum(1);
        DataStream<Tuple2<Integer, Integer>> b = filterDs.rebalance().map(
                new RichMapFunction<Long, Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> map(Long aLong) throws Exception {
                        int id = getRuntimeContext().getIndexOfThisSubtask();
                        return Tuple2.of(id, 1);
                    }
                }
        ).keyBy(t -> t.f0).sum(1);

        // 4.sink
        a.print("A@");
        b.print("B@");

        // 5.execute
        env.execute();
    }
}


