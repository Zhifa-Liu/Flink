package cn.edu.neu.batch_stream_api.connnectors;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;

/**
 * @author 32098
 *
 * 使用 RedisCommand 设置的数据结构类型和 redis 结构的对应关系：
 * `Data type of redis`                     `RedisCommand`
 * HASH                                     HSET
 * LIST                                     RPUSH,LPUSH
 * SET                                      SADD
 * PUBSUB                                   PUBLISH
 * STRING                                   SET
 * HYPER_LOG_LOG                            PFADD
 * SORTED_SET                               ZADD
 * SORTED_SET                               ZREM
 */
public class ConnectorRedis {
    /**
     * -2.定义一个Mapper用来指定存储到Redis中的数据结构
     */
    public static class RedisWordCountMapper implements RedisMapper<Tuple2<String, Integer>> {
        @Override
        public RedisCommandDescription getCommandDescription() {
            // 设置使用的 redis 数据结构类型(通过 RedisCommand 设置)，和 key 的名称
            return new RedisCommandDescription(RedisCommand.HSET, "WordCount");
        }
        @Override
        public String getKeyFromData(Tuple2<String, Integer> data) {
            // 设置键值对key的值
            return data.f0;
        }
        @Override
        public String getValueFromData(Tuple2<String, Integer> data) {
            // 设置键值对value的值
            return data.f1.toString();
        }
    }


    public static void main(String[] args) throws Exception {
        // 1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2.source
        DataStream<String> linesDs = env.socketTextStream("master", 9999);

        // 3.transformation
        // 3.1 切割并记为1
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOneDs = linesDs.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        });
        // 3.2 分组
        KeyedStream<Tuple2<String, Integer>, Tuple> groupedDs = wordAndOneDs.keyBy(0);
        // 3.3 聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = groupedDs.sum(1);

        // 4.Sink
        result.print();
        // * 最后将结果保存到Redis
        // * 注意:存储到Redis的数据结构:使用hash也就是map
        // * key            value
        // * WordCount      (单词,数量)

        // -1.创建RedisSink之前需要创建RedisConfig
        // 连接单机版Redis
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("127.0.0.1").build();

        // -3.创建并使用RedisSink
        result.addSink(new RedisSink<Tuple2<String, Integer>>(conf, new RedisWordCountMapper()));

        //5.execute
        env.execute();
    }
}
/*
        // 连接集群版Redis
        // HashSet<InetSocketAddress> nodes = new HashSet<>(Arrays.asList(new InetSocketAddress(InetAddress.getByName("node1"), 6379),new InetSocketAddress(InetAddress.getByName("node2"), 6379),new InetSocketAddress(InetAddress.getByName("node3"), 6379)));
        // FlinkJedisClusterConfig conf2 = new FlinkJedisClusterConfig.Builder().setNodes(nodes).build();
        // 连接哨兵版Redis
        // Set<String> sentinels = new HashSet<>(Arrays.asList("mnode1:26379", "node2:26379", "node3:26379"));
        // FlinkJedisSentinelConfig conf3 = new FlinkJedisSentinelConfig.Builder().setMasterName("mymaster").setSentinels(sentinels).build();
 */


