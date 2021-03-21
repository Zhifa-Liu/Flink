package cn.edu.neu.batch_stream_api.transformation;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author 32098
 *
 * split: 分流 (已过期并移除)
 * select: 获取分流后的数据
 * side outputs: 使用process方法对流中数据进行处理，并针对不同的处理结果将数据收集到不同的OutputTag
 *
 * Desc requirement:
 * 奇偶数分流并get
 */
public class SplitSelectSideOutputs {
    public static void main(String[] args) throws Exception {
        // 1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        // 2.source
        DataStreamSource<Integer> ds = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        // 3.transformation
        OutputTag<Integer> even = new OutputTag<Integer>("even", TypeInformation.of(Integer.class));
        OutputTag<Integer> odd = new OutputTag<Integer>("odd", TypeInformation.of(Integer.class));

        SingleOutputStreamOperator<Integer> result = ds.process(new ProcessFunction<Integer, Integer>() {
            @Override
            public void processElement(Integer integer, Context context, Collector<Integer> collector) throws Exception {
                if (integer % 2 == 0){
                    context.output(even, integer);
                } else {
                    context.output(odd, integer);
                }
            }
        });

        DataStream<Integer> evenOut = result.getSideOutput(even);
        DataStream<Integer> oddOut = result.getSideOutput(odd);

        // 4.sink
        evenOut.print("Even#");
        oddOut.print("Odd#");

        // 5.execute
        env.execute();
    }
}

