package cn.edu.neu.advance_api.time_watermaker;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * @author 32098
 *
 * Desc requirement:
 * 模拟实时订单数据，格式为: (订单ID，用户ID，时间戳/事件时间，订单金额)，
 * 并每隔5s，计算5秒内每个用户的订单总金额；
 * 且添加 Watermaker 来解决一定程度上的数据延迟和数据乱序问题；
 * 且使用 OutputTag+allowedLateness 解决数据丢失问题。
 *
 * more important
 */
public class WatermakerAllowedLateness {
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
        // 2.source
        // 模拟实时订单数据并模拟数据延迟和乱序
        DataStreamSource<Order> orderDs = env.addSource(new SourceFunction<Order>() {
            private boolean flag = true;
            @Override
            public void run(SourceContext<Order> ctx) throws Exception {
                Random random = new Random();
                while (flag) {
                    String orderId = UUID.randomUUID().toString();
                    int userId = random.nextInt(3);
                    int money = random.nextInt(100);
                    // 模拟数据延迟和乱序
                    long eventTime = System.currentTimeMillis() - random.nextInt(10) * 1000;
                    ctx.collect(new Order(orderId, userId, money, eventTime));
                    TimeUnit.MICROSECONDS.sleep(100);
                }
            }
            @Override
            public void cancel() {
                flag = false;
            }
        });

        // 3.transformation

        // 添加上 Watermaker
        DataStream<Order> watermakerDs = orderDs.assignTimestampsAndWatermarks(
            WatermarkStrategy.<Order>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner((event, timestamp) -> event.getEventTime())
        );

        OutputTag<Order> outputTag = new OutputTag<>("SeriouslyLate", TypeInformation.of(Order.class));

        SingleOutputStreamOperator<Order> result = watermakerDs
                .keyBy(Order::getUserId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .allowedLateness(Time.seconds(5))
                .sideOutputLateData(outputTag)
                .sum("money");

        DataStream<Order> resultSide = result.getSideOutput(outputTag);

        // 4.sink
        result.print("正常数据&迟到不严重数据:");
        resultSide.printToErr("     迟到比较严重的数据:");

        // 5.execute
        env.execute();
    }
}


