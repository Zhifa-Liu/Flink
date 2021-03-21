package cn.edu.neu.state_management;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author 32098
 *
 * Desc requirement:
 * 使用KeyedState中的ValueState获取数据中的最大值(实际中直接使用maxBy即可)
 *
 * wordcount：sum(StreamGroupedReduce) keyed state 的使用
 */
public class KeyedState {
    public static void main(String[] args) {
        // 1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 并行度设置为1，方便观察
        env.setParallelism(1);

        // 2.Source
        DataStreamSource<Tuple2<String, Long>> tupleDs = env.fromElements(
                Tuple2.of("北京", 1L),
                Tuple2.of("上海", 2L),
                Tuple2.of("北京", 6L),
                Tuple2.of("上海", 8L),
                Tuple2.of("北京", 3L),
                Tuple2.of("上海", 4L)
        );

        // 3.Transformation：使用KeyedState中的ValueState获取流数据中的最大值

        // 实现方式A:直接使用maxBy--开发中使用该方式即可>>>
        // min只会求出最小的那个字段,其他的字段不管
        // minBy会求出最小的那个字段和对应的其他的字段
        // max只会求出最大的那个字段,其他的字段不管
        // maxBy会求出最大的那个字段和对应的其他的字段
        SingleOutputStreamOperator<Tuple2<String, Long>> result = tupleDs.keyBy(t -> t.f0).maxBy(1);

        // 实现方式B:使用KeyState中的ValueState>>>学习测试时使用or在后续项目中/实际开发中遇到复杂的且Flink没有实现的逻辑,才用该方式!
        SingleOutputStreamOperator<Tuple3<String, Long, Long>> result2 = tupleDs.keyBy(t -> t.f0)
                .map(new RichMapFunction<Tuple2<String, Long>, Tuple3<String, Long, Long>>() {
                    // -a.定义状态用来存储最大值
                    private ValueState<Long> maxValueState = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // -b.定义状态描述符:描述状态的名称和里面的数据类型
                        ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<Long>("maxValueState", Long.class);
                        // -c.根据状态描述符初始化状态
                        maxValueState = getRuntimeContext().getState(descriptor);
                    }

                    @Override
                    public Tuple3<String, Long, Long> map(Tuple2<String, Long> value) throws Exception {
                        // -d.使用State,取出State中的最大值/历史最大值
                        Long historyMaxValue = maxValueState.value();
                        Long currentValue = value.f1;
                        if (historyMaxValue == null || currentValue > historyMaxValue) {
                            // -e.更新状态,把当前的作为新的最大值存到状态中
                            maxValueState.update(currentValue);
                            return Tuple3.of(value.f0, currentValue, currentValue);
                        } else {
                            return Tuple3.of(value.f0, currentValue, historyMaxValue);
                        }
                    }
                });
    }
}
