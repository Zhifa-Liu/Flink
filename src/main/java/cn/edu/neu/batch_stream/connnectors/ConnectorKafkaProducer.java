package cn.edu.neu.batch_stream.connnectors;

import com.alibaba.fastjson.JSON;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * @author 32098
 *
 * make sure package connector_jar(see pom.xml: flink-connector and plugins) in project jar
 */
public class ConnectorKafkaProducer {
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Student {
        private Integer id;
        private String name;
        private Integer age;
    }

    public static void main(String[] args) throws Exception {
        // 1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2.source
        DataStreamSource<Student> studentDs = env.fromElements(new Student(1, "tonyma", 18));

        // 3.transformation
        // 目前，我们使用Kafka设置的序列化和反序列化规则都是SimpleStringSchema，所以先将Student转为字符串
        SingleOutputStreamOperator<String> jsonDs = studentDs.map(new MapFunction<Student, String>() {
            @Override
            public String map(Student value) throws Exception {
                // return str = value.toString();
                return JSON.toJSONString(value);
            }
        });

        // 4.sink
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "master:9092");
        FlinkKafkaProducer<String> kafkaSink = new FlinkKafkaProducer<>(
                "flink_kafka",
                // 序列化规则
                new SimpleStringSchema(),
                props
        );
        jsonDs.addSink(kafkaSink);

        //5.execute
        env.execute();
    }
}

/*
Start kafka server:
/usr/local/zookeeper/bin/zkServer.sh start
kafka-server-start.sh config/server.properties

All topic in kafka server:
kafka-topics.sh --list --bootstrap-server master:9092

Create topic "flink_kafka": 3 partition
kafka-topics.sh --create --bootstrap-server master:9092 --replication-factor 1 --partitions 3 --topic flink_kafka

Check information of "flink_kafka":
kafka-topics.sh --topic flink_kafka --describe --bootstrap-server master:9092

Shell consume data produced by this class(we can also consume by ConnectKafkaConsumer):
kafka-console-consumer.sh --bootstrap-server master:9092 --topic flink_kafka

Also can produce data by order below:
kafka-console-consumer.sh --bootstrap-server master:9092 --topic flink_kafka --from-beginning

Modify partitions by order below:
kafka-topics.sh --alter --partitions 4 --topic flink_kafka --bootstrap-server master:9092
 */



