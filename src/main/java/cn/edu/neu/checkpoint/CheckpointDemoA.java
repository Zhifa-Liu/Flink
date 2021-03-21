package cn.edu.neu.checkpoint;

import org.apache.commons.lang3.SystemUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * @author 32098
 *
 * 配置checkpoint参数：全局配置请修改flink-conf.yaml
 */
public class CheckpointDemoA {
    public static void main(String[] args) throws Exception {
        // 获取项目路径
        String path = System.getProperty("user.dir");

        // 1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 1.1 Checkpoint参数设置
        // ===========类型1:必须参数=============
        // 设置Checkpoint的时间间隔为1000ms做一次Checkpoint/其实就是每隔1000ms发一次Barrier!
        env.enableCheckpointing(1000);
        // 设置State状态存储介质
        /*
        if(args.length > 0){
            env.setStateBackend(new FsStateBackend(args[0]));
        }else {
            env.setStateBackend(new FsStateBackend("file:///"+path+"/data/checkpoint"));
        }
        */
        if (SystemUtils.IS_OS_WINDOWS) {
            env.setStateBackend(new FsStateBackend("file:///"+path+"/data/checkpoint"));
        } else {
            env.setStateBackend(new FsStateBackend("hdfs://mnode1:8020/flink-checkpoint/checkpoint"));
        }

        // ===========类型2:建议参数===========
        // 设置两个Checkpoint 之间最少等待时间,如设置Checkpoint之间最少是要等 500ms (为了避免每隔1000ms做一次Checkpoint的时候,前一次太慢和后一次重叠到一起去了)
        // 如:高速公路上,每隔1s关口放行一辆车,但是规定了两车之间的最小车距为500m
        // 默认是0
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        // 设置如果在做Checkpoint过程中出现错误，是否让整体任务失败：true是  false不是
        // 默认是true
        // env.getCheckpointConfig().setFailOnCheckpointingErrors(false);
        // 默认值为0，表示不容忍任何检查点失败
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(10);
        // 设置是否清理检查点,表示 Cancel 时是否需要保留当前的 Checkpoint，默认 Checkpoint会在作业被Cancel时被删除
        // ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION：true，当作业被取消时，删除外部的checkpoint(默认值)
        // ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION：false，当作业被取消时，保留外部的checkpoint
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // ===========类型3:直接使用默认的即可===============
        // 设置checkpoint的执行模式为EXACTLY_ONCE(默认)
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 设置checkpoint的超时时间,如果 Checkpoint在 60s 内尚未完成说明该次Checkpoint失败,则丢弃。
        // 默认10分钟
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        // 设置同一时间有多少个checkpoint可以同时执行
        // 默认为1
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        // 2.Source
        DataStream<String> linesDs = env.socketTextStream("master", 9999);

        // 3.Transformation
        // 3.1切割出每个单词并直接记为1
        DataStream<Tuple2<String, Integer>> wordAndOneDs = linesDs.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                //value就是每一行
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        });
        // 3.2 分组
        // 注意：批处理的分组是groupBy，流处理的分组是keyBy
        KeyedStream<Tuple2<String, Integer>, String> groupedDs = wordAndOneDs.keyBy(t -> t.f0);
        // 3.3 聚合
        DataStream<Tuple2<String, Integer>> aggResult = groupedDs.sum(1);

        DataStream<String> result = (SingleOutputStreamOperator<String>) aggResult.map(new RichMapFunction<Tuple2<String, Integer>, String>() {
            @Override
            public String map(Tuple2<String, Integer> value) throws Exception {
                return value.f0 + ":::" + value.f1;
            }
        });

        // 4.sink
        result.print();

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "master:9092");
        FlinkKafkaProducer<String> kafkaSink = new FlinkKafkaProducer<>("flink_kafka", new SimpleStringSchema(), props);
        result.addSink(kafkaSink);

        //5.execute
        env.execute();

        // kafka-console-consumer.sh --bootstrap-server master:9092 --topic flink_kafka
    }
}
