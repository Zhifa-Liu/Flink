package cn.edu.neu.advance_api.time_watermaker;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * @author 32098
 *
 * Desc requirement:
 * 模拟实时订单数据，格式为: (订单ID，用户ID，时间戳/事件时间，订单金额)，
 * 并每隔5s，计算5秒内每个用户的订单总金额
 * 且添加 Watermaker 来解决一定程度上的数据延迟和数据乱序问题。
 *
 * Important
 */
public class WatermakerDevelop {
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Order {
        private String orderId;
        private Integer userId;
        private Integer money;
        private Long eventTime;
    }

    public static void main(String[] args) throws Exception {
        // 1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2.Source
        // 模拟实时订单数据并模拟数据延迟和乱序
        DataStream<Order> orderDs = env.addSource(new SourceFunction<Order>() {
            private boolean flag = true;

            @Override
            public void run(SourceContext<Order> ctx) throws Exception {
                Random random = new Random();
                while (flag) {
                    String orderId = UUID.randomUUID().toString();
                    int userId = random.nextInt(3);
                    int money = random.nextInt(100);
                    // 模拟数据延迟与乱序
                    long eventTime = System.currentTimeMillis() - random.nextInt(5) * 1000;
                    ctx.collect(new Order(orderId, userId, money, eventTime));

                    TimeUnit.SECONDS.sleep(1);
                }
            }

            @Override
            public void cancel() {
                flag = false;
            }
        });

        // 3.Transformation

        /*
        // 过期
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime); //新版本默认就是EventTime
        DataStream<Order> watermakerDS = orderDs.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<Order>(Time.seconds(3)) {
                    @Override
                    public long extractTimestamp(Order element) {
                        // 指定事件时间是哪一列，Flink底层会自动计算 Watermaker：当前最大的事件时间 - 最大允许的延迟时间或乱序时间
                        return element.eventTime;
                    }
        });
         */

        // 添加上 Watermaker
        DataStream<Order> watermakerDs = orderDs.assignTimestampsAndWatermarks(
            WatermarkStrategy.<Order>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner((event, timestamp) -> event.getEventTime())
        );

        DataStream<Order> result = watermakerDs
                .keyBy(Order::getUserId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .sum("money");

        // 4.Sink
        result.print();

        // 5.execute
        env.execute();
    }
}