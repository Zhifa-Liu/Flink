package cn.edu.neu.advance_api.window;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author 32098
 *
 * Desc requirement：
 * 数据为（信号灯编号，通过该信号灯的车的数量）：
 * 9,3
 * 9,2
 * 9,7
 * 4,9
 * 2,6
 * 1,5
 * 2,3
 * 5,7
 * 5,4
 * 通过：nc -lk 9999 输入
 *
 * 基于时间的滑动窗口：每5秒钟统计一次，最近10秒钟内，各个路口通过红绿灯汽车的数量
 * 基于时间的滚动窗口：每5秒钟统计一次，最近5秒钟内，各个路口通过红绿灯汽车的数量
 *
 * More important than count based window
 */
public class TimeWindow {
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class CartInfo {
        // 信号灯id
        private String sensorId;
        // 通过该信号灯的车的数量
        private Integer count;
    }

    public static void main(String[] args) throws Exception {
        // 1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2.Source
        DataStreamSource<String> socketDs = env.socketTextStream("master", 9999);

        // 3.Transformation
        SingleOutputStreamOperator<CartInfo> cartInfoDs = socketDs.map(
            new MapFunction<String, CartInfo>() {
                @Override
                public CartInfo map(String value) throws Exception {
                    String[] arr = value.split(",");
                    return new CartInfo(arr[0], Integer.parseInt(arr[1]));
                }
            }
        );

        // 基于时间的滚动窗口：
        // TumblingProcessingTimeWindows 滚动处理时间窗口
//        SingleOutputStreamOperator<CartInfo> tumblingResult = cartInfoDs.keyBy(CartInfo::getSensorId)
//                .window(TumblingProcessingTimeWindows.of(Time.seconds(5))).sum("count");
        // cartInfoDs.keyBy(CartInfo::getSensorId).timeWindow(Time.seconds(5)); 过期

        // 基于时间的滑动窗口:
        // SlidingProcessingTimeWindows 滑动处理时间窗口
        SingleOutputStreamOperator<CartInfo> slidingResult = cartInfoDs.keyBy(CartInfo::getSensorId)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5))).sum("count");
        // cartInfoDs.keyBy(CartInfo::getSensorId).timeWindow(Time.seconds(10), Time.seconds(5)); 过期

        // 4.sink
        // tumblingResult.print("Tumbling>>>");
        slidingResult.print(" Sliding>>>");

        // 5.execute
        env.execute();
    }
}

