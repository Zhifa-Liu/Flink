package cn.edu.neu.experiment.advertise_blacklist;

import cn.edu.neu.experiment.AdvertiseClickBean;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.Properties;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author 32098
 */
public class KafkaAdvertiseDataConsumerB {
    public static void main(String[] args) throws Exception {
        Properties pros = new Properties();
        pros.setProperty("bootstrap.servers", "master:9092");
        pros.setProperty("group.id", "flink");
        pros.setProperty("auto.offset.reset","latest");
        pros.setProperty("flink.partition-discovery.interval-millis","5000");
        pros.setProperty("enable.auto.commit", "true");
        pros.setProperty("auto.commit.interval.ms", "2000");

        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<String>(
                "flink_kafka",
                new SimpleStringSchema(),
                pros
        );
        kafkaSource.setStartFromLatest();

        // 1. env：创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 2. source
        DataStreamSource<String> kafkaDataStream = env.addSource(kafkaSource);

        // 3. transformation
        // 3.1 to java object
        SingleOutputStreamOperator<AdvertiseClickBean> advertiseClickDataStream = kafkaDataStream.map(new MapFunction<String, AdvertiseClickBean>() {
            @Override
            public AdvertiseClickBean map(String s) throws Exception {
                return JSON.parseObject(s, AdvertiseClickBean.class);
            }
        });

        // 3.2 添加水位线
        DataStream<AdvertiseClickBean> adClickDataStream  =  advertiseClickDataStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<AdvertiseClickBean>forBoundedOutOfOrderness(Duration.ofSeconds(5)).withTimestampAssigner((adClickData, timestamp) -> adClickData.getClickTime())
        );

        // 3.3 map: 处理时间并选取需要的数据
        SingleOutputStreamOperator<AdvertiseClickData> dealtAdClickDs = adClickDataStream.map(new MapFunction<AdvertiseClickBean, AdvertiseClickData>() {
            @Override
            public AdvertiseClickData map(AdvertiseClickBean advertiseClickBean) throws Exception {
                String ymd = new SimpleDateFormat("yyyy-MM-dd").format(new Date(advertiseClickBean.getClickTime()));
                return new AdvertiseClickData(ymd, advertiseClickBean.getClickUserId(), advertiseClickBean.getAdvertiseId(), 1);
            }
        });

        // 3.4 创建视图
        tEnv.createTemporaryView("advertise_click_data",
                dealtAdClickDs,
                $("clickTime"),
                $("clickUserId"),
                $("advertiseId"),
                $("clickCount")
        );

        // 3.5 分组聚合
        Table resultTable = tEnv.sqlQuery(
                "SELECT clickTime, clickUserId, advertiseId, SUM(clickCount) as clickCount FROM advertise_click_data GROUP BY clickTime, clickUserId, advertiseId"
        );

        // 3.6
        DataStream<AdvertiseClickData> resultDs = tEnv.toRetractStream(resultTable, AdvertiseClickData.class).filter(record->record.f0).map(record->record.f1);

        // 4. sink
        resultDs.addSink(new MysqlSink());

        // 5. execute
        env.execute();
    }
}

