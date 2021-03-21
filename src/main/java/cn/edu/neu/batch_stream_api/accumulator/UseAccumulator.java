package cn.edu.neu.batch_stream_api.accumulator;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.configuration.Configuration;

/**
 * @author 32098
 */
public class UseAccumulator {
    public static void main(String[] args) throws Exception {
        // 1.env
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 2.source
        DataSource<String> dataDs = env.fromElements("aaa", "bbb", "ccc", "ddd");

        // 3.transformation
        MapOperator<String, String> result = dataDs.map(
                new RichMapFunction<String, String>() {
                    // 创建累加器
                    final IntCounter elementCounter = new IntCounter();
                    Integer count = 0;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        // 注册累加器
                        getRuntimeContext().addAccumulator("elementCounter", elementCounter);
                    }

                    @Override
                    public String map(String s) throws Exception {
                        // 使用累加器
                        this.elementCounter.add(1);
                        count += 1;
                        System.out.println("不使用累加器统计的结果:"+count);
                        return s;
                    }
                }
        ).setParallelism(2);

        // 4. sink
        result.writeAsText("data/accumulator");

        // 5. execute
        JobExecutionResult jobResult = env.execute();
        // 获取累加器结果
        int nums = jobResult.getAccumulatorResult("elementCounter");
        System.out.println("使用累加器统计的结果:"+nums);
    }
}



