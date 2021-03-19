package cn.edu.neu.batch_stream.connnectors;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * @author 32098
 */
public class ConnectorKafkaConsumer {
    public static void main(String[] args) throws Exception {
        // 1. env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. source
        Properties pros = new Properties();
        // 2.1 消费者属性之集群地址
        pros.setProperty("bootstrap.servers", "master:9092");
        // 2.2 消费者属性之组id(如果不设置，会有默认组id，但是不方便管理)
        pros.setProperty("group.id", "flink");
        // 2.3 消费者属性之offset重置规则
        pros.setProperty("auto.offset.reset","latest");
        // 2.4 动态分区检测：开启一个后台线程每5s检测一下kafka分区情况
        pros.setProperty("flink.partition-discovery.interval-millis","5000");
        // 2.5 自动提交 offset：若设置了 Checkpoint，那么 offset 会随做 Checkpoint 的时候提交到 Checkpoint 与 默认主题
        pros.setProperty("enable.auto.commit", "true");
        // 2.6 自动提交间隔
        pros.setProperty("auto.commit.interval.ms", "2000");

        // KafkaConsumer 消费到的数据就是 Source
        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<String>(
                // 订阅的主题：kafka-topic
                "flink_kafka",
                // 反序列化规则
                new SimpleStringSchema(),
                pros
        );

        // 2.7 设置从哪开始消费：发生作业故障时，作业从checkpoint自动恢复，消费位置从保存的状态中恢复，与下面设置无关。
        // 设置直接从 Earliest 消费，该设置与 auto.offset.reset 配置无关
        kafkaSource.setStartFromEarliest();
        // 重新设置从 kafka 记录的 group.id 的位置开始消费，如果没有记录则从 auto.offset.reset 配置开始消费
        kafkaSource.setStartFromGroupOffsets();

        // 2.8 添加 DataStreamSource
        DataStreamSource<String> kafkaDs = env.addSource(kafkaSource);

        // 3. transformation
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordCountResult = kafkaDs.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        }).keyBy(t -> t.f0).sum(1);

        // 4. sink
        wordCountResult.print();

        // 5. execute
        env.execute();
    }
}

/*
实际生产环境需求>>>
场景一：topic 发现
    有一个 Flink 作业需要将五份数据聚合到一起，五份数据对应五个 kafka topic。
随着业务增长，新增一类数据，同时新增了一个 kafka topic，如何在不重启作业的情
况下作业自动感知新的 topic。
场景二：partition 发现
    作业从一个固定的 kafka topic 读数据，开始该 topic 有 10 个 partition。随着
业务的增长与数据量的变大，需要对 kafka partition 个数进行扩容，由 10 个扩容到
20 个，该情况下如何在不重启作业情况下动态感知新扩容的 partition？

    针对上面的两种场景，首先需要在构建 FlinkKafkaConsumer 时的 properties 中设置
flink.partition-discovery.interval-millis 参数为非负值，表示开启动态发现的开关。
此时 FlinkKafkaConsumer 内部会启动一个单独的线程定期去 kafka 获取最新的 meta 信息。
针对场景一：
    topic 还需传一个正则表达式描述的 pattern，每次获取最新 kafka meta 时获取正则
匹配的最新 topic 列表。
针对场景二：
    只需设置前面的动态发现参数，就会在定期获取 kafka 最新 meta 信息时匹配新的 partition。
为了保证数据的正确性，`新发现的 partition 从最早的位置开始读取`。
 */

