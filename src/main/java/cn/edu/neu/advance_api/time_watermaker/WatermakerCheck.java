package cn.edu.neu.advance_api.time_watermaker;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang.time.FastDateFormat;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
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
 * Just know it
 */
public class WatermakerCheck {
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
        FastDateFormat df = FastDateFormat.getInstance("HH:mm:ss");

        // 1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2.Source
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
                    long eventTime = System.currentTimeMillis() - random.nextInt(5) * 1000;
                    System.out.println("发送的数据为>>> " + userId + " : " + df.format(eventTime));
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
        // For development
        DataStream<Order> watermakerDs = orderDs.assignTimestampsAndWatermarks(
            WatermarkStrategy.<Order>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner((event, timestamp) -> event.getEventTime())
        );
        */

        // 添加上 Watermaker
        DataStream<Order> watermakerDs = orderDs.assignTimestampsAndWatermarks(
            new WatermarkStrategy<Order>() {
                @Override
                public WatermarkGenerator<Order> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                    return new WatermarkGenerator<Order>() {
                        private int userId = 0;
                        private long eventTime = 0L;
                        private final long outOfOrdernessMillis = 3000;
                        private long maxTimestamp = Long.MIN_VALUE + outOfOrdernessMillis + 1;

                        @Override
                        public void onEvent(Order event, long eventTimestamp, WatermarkOutput output) {
                            userId = event.userId;
                            eventTime = event.eventTime;
                            maxTimestamp = Math.max(maxTimestamp, eventTimestamp);
                        }

                        @Override
                        public void onPeriodicEmit(WatermarkOutput output) {
                            // Watermaker = 当前最大事件时间 - 最大允许的延迟时间或乱序时间
                            Watermark watermark = new Watermark(maxTimestamp - outOfOrdernessMillis - 1);
                            System.out.println("Key:" + userId + ",系统时间:" + df.format(System.currentTimeMillis()) + ",事件时间:" + df.format(eventTime) + ",水印时间:" + df.format(watermark.getTimestamp()));
                            output.emitWatermark(watermark);
                        }
                    };
                }
            }.withTimestampAssigner((event, timestamp) -> event.getEventTime())
        );

        /*
        For development
        DataStream<Order> result = watermakerDs
                .keyBy(Order::getUserId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .sum("money");
         */

        // 学习测试时使用下面的代码对数据进行更详细的输出，如输出窗口触发时各个窗口中的数据的事件时间，Watermaker 时间
        DataStream<String> result = watermakerDs
                .keyBy(Order::getUserId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                // 把apply中的函数应用在窗口中的数据上
                // WindowFunction<IN, OUT, KEY, W extends Window>
                .apply(new WindowFunction<Order, String, Integer, TimeWindow>() {
                    @Override
                    public void apply(Integer key, TimeWindow window, Iterable<Order> input, Collector<String> out) throws Exception {
                        // 准备一个集合用来存放属于该窗口的数据的事件时间
                        List<String> eventTimeList = new ArrayList<>();
                        for (Order order : input) {
                            Long eventTime = order.eventTime;
                            eventTimeList.add(df.format(eventTime));
                        }
                        String outStr = String.format(
                                "Key:%s，窗口开始结束:[%s~%s)，属于该窗口的事件时间:%s",
                                key.toString(), df.format(window.getStart()), df.format(window.getEnd()), eventTimeList
                        );
                        out.collect(outStr);
                    }
                });

        // 4.Sink
        result.print();

        // 5.execute
        env.execute();
    }
}

