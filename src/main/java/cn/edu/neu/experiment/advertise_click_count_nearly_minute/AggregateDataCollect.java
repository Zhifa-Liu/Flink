package cn.edu.neu.experiment.advertise_click_count_nearly_minute;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Long: click time
 * Tuple2<String, Long>: Tuple2.of(advertiseId, click time)
 * String: key => advertiseId
 * @author 32098
 */
public class AggregateDataCollect implements WindowFunction<Long, Tuple2<String, Long>, String, TimeWindow> {

    /**
     *
     * @param s key => advertiseId
     * @param timeWindow timeWindow
     * @param input click time
     * @param collector collector
     * @throws Exception Exception
     */
    @Override
    public void apply(String s, TimeWindow timeWindow, Iterable<Long> input, Collector<Tuple2<String, Long>> collector) throws Exception {
        long clickTime = input.iterator().next();
        collector.collect(Tuple2.of(s, clickTime));
    }
}