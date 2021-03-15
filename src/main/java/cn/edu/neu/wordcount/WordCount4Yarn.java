package cn.edu.neu.wordcount;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * Desc
 * 需求:使用Flink完成WordCount-DataStream--使用lambda表达式--修改代码使适合在Yarn上运行
 * 编码步骤
 * 1.准备环境-env
 * 2.准备数据-source
 * 3.处理数据-transformation
 * 4.输出结果-sink
 * 5.触发执行-execute//批处理不需要调用!流处理需要
 * @author 32098
 */
public class WordCount4Yarn {
    public static void main(String[] args) throws Exception {
        //获取参数
        ParameterTool params = ParameterTool.fromArgs(args);
        String output = null;
        if (params.has("output")) {
            output = params.get("output");
        } else {
            output = "hdfs://master:9000/flink/example/wordcount/output_" + System.currentTimeMillis();
        }

        // 1.准备环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        // env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        // env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        // 2.准备数据-source
        DataStream<String> linesDS = env.fromElements("nhbd hadoop spark", "nhbd hadoop spark", "nhbd hadoop", "nhbd");

        // 3.处理数据-transformation
        DataStream<Tuple2<String, Integer>> result = linesDS
                .flatMap(
                        (String value, Collector<String> out) -> Arrays.stream(value.split(" ")).forEach(out::collect)
                ).returns(Types.STRING)
                .map(
                        (String value) -> Tuple2.of(value, 1)
                ).returns(Types.TUPLE(Types.STRING, Types.INT))
                //.keyBy(0);
                .keyBy((KeySelector<Tuple2<String, Integer>, String>) t -> t.f0)
                .sum(1);

        // 4.输出结果-sink
        result.print();

        // 如果执行报hdfs权限相关错误,可以执行 hadoop fs -chmod -R 777  /
        // 设置用户名
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        // result.writeAsText("hdfs://master:9000/flink/example/wordcount/output_"+System.currentTimeMillis()).setParallelism(1);
        result.writeAsText(output).setParallelism(1);

        // 5.触发执行-execute
        env.execute();
    }
}
