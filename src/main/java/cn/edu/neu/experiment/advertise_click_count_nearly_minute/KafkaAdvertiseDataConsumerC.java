package cn.edu.neu.experiment.advertise_click_count_nearly_minute;

import cn.edu.neu.experiment.AdvertiseClickBean;
import com.alibaba.fastjson.JSON;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.Properties;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author 32098
 */
public class KafkaAdvertiseDataConsumerC {
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class TimeClickData{
        private Long clickTime;
        private String dealtTime;
        private Long click;
    }

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

        // 3.3 处理事件时间：处理如下
        /*
        9s(1-10) => 10s
        13s(11-20) => 20s
        24s(21-30) => 30s
        32s(31-40) => 40s
        48s(41-50) => 50s
        56s(51-60) => 60s(0)
        (s / 10 (整除) + 1)*10 : (56/10+1)=60
         */
        KeyedStream<Tuple2<String, Long>, String> adClickTimeKeyedDs = adClickDataStream.map(new MapFunction<AdvertiseClickBean, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(AdvertiseClickBean advertiseClickBean) throws Exception {
                long ts = advertiseClickBean.getClickTime();
                String time = new SimpleDateFormat("HH:mm:ss").format(new Date(ts));
                String[] hms = time.split(":");
                int s = (Integer.parseInt(hms[2])/10+1)*10;
                int m = Integer.parseInt(hms[1]);
                int h = Integer.parseInt(hms[0]);
                if(s == 60){
                    m = m + 1;
                    s = 0;
                    if(m == 60){
                        h = h + 1;
                        if(h == 24){
                            h = 0;
                        }
                    }
                }
                String hStr, mStr, sStr;
                if(h < 10){
                    hStr = "0" + h;
                }else{
                    hStr = String.valueOf(h);
                }
                if(m < 10){
                    mStr = "0" + m;
                }else{
                    mStr = String.valueOf(m);
                }
                if(s == 0){
                    sStr = "00";
                }else{
                    sStr = String.valueOf(s);
                }
                String hmsNew = hStr+":"+mStr+":"+sStr;
                return Tuple2.of(hmsNew, 1L);
            }
        }).keyBy(e -> e.f0);

        SingleOutputStreamOperator<Tuple2<String, Long>> resultA = adClickTimeKeyedDs.window(SlidingEventTimeWindows.of(Time.seconds(60), Time.seconds(10))).sum(1);
        SingleOutputStreamOperator<Tuple2<String, Long>> resultB = adClickTimeKeyedDs.window(SlidingEventTimeWindows.of(Time.seconds(60), Time.seconds(10))).reduce(new ReduceFunction<Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> reduce(Tuple2<String, Long> valueA, Tuple2<String, Long> valueB) throws Exception {
                return Tuple2.of(valueA.f0, valueA.f1+valueB.f1);
            }
        });
        SingleOutputStreamOperator<Tuple2<String, Long>> resultC = adClickTimeKeyedDs.window(SlidingEventTimeWindows.of(Time.seconds(60), Time.seconds(10)))
                .aggregate(new ClickTimeAggregate(), new AggregateDataCollect());
        SingleOutputStreamOperator<Tuple2<String, Long>> resultD = adClickTimeKeyedDs.window(SlidingEventTimeWindows.of(Time.seconds(60), Time.seconds(10)))
                .process(new WindowDataProcess());

        // 4. sink
        resultC.addSink(new JsonSink());

        resultA.print();
        resultB.print();
        // resultC.print();
        resultD.print();

//        // 3~ transformation
//        SingleOutputStreamOperator<TimeClickData> adClickTimeDs = adClickDataStream.map(new MapFunction<AdvertiseClickBean, TimeClickData>() {
//            @Override
//            public TimeClickData map(AdvertiseClickBean advertiseClickBean) throws Exception {
//                long ts = advertiseClickBean.getClickTime();
//                String time = new SimpleDateFormat("HH:mm:ss").format(new Date(ts));
//                String[] hms = time.split(":");
//                int s = (Integer.parseInt(hms[2])/10+1)*10;
//                int m = Integer.parseInt(hms[1]);
//                int h = Integer.parseInt(hms[0]);
//                if(s == 60){
//                    m = m + 1;
//                    s = 0;
//                    if(m == 60){
//                        h = h + 1;
//                        if(h == 24){
//                            h = 0;
//                        }
//                    }
//                }
//                String hStr, mStr, sStr;
//                if(h < 10){
//                    hStr = "0" + h;
//                }else{
//                    hStr = String.valueOf(h);
//                }
//                if(m < 10){
//                    mStr = "0" + m;
//                }else{
//                    mStr = String.valueOf(m);
//                }
//                if(s == 0){
//                    sStr = "00";
//                }else{
//                    sStr = String.valueOf(s);
//                }
//                String hmsNew = hStr+":"+mStr+":"+sStr;
//                return new TimeClickData(ts, hmsNew, 1L);
//            }
//        });
//
//        tEnv.createTemporaryView("t_time_click", adClickTimeDs, $("clickTime").rowtime(), $("dealtTime"), $("click"));
//        Table tempTable = tEnv.sqlQuery("SELECT dealtTime, count(click) as total_click FROM t_time_click GROUP BY dealtTime, HOP(clickTime, interval '10' SECOND, interval '60' SECOND) ORDER BY dealtTime DESC LIMIT 24");
//        SingleOutputStreamOperator<Row> resultStream = tEnv.toRetractStream(tempTable, Row.class).filter(f -> f.f0).map(f -> f.f1);
//
//        // 4~ sink
//        resultStream.print();

        // 5. execute
        env.execute();
    }
}


