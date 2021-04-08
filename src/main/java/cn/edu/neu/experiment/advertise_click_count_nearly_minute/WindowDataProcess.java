package cn.edu.neu.experiment.advertise_click_count_nearly_minute;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;
import java.util.TreeMap;

/**
 * @author 32098
 */
public class WindowDataProcess extends ProcessWindowFunction<Tuple2<String, Long>, Tuple2<String, Long>, String, TimeWindow> {

    @Override
    public void process(String s, Context context, Iterable<Tuple2<String, Long>> inputs, Collector<Tuple2<String, Long>> collector) throws Exception {
        Map<String, Long> adAndClickTime = new TreeMap<>();

        for (Tuple2<String, Long> input : inputs) {
            String key = input.f0;
            if(adAndClickTime.containsKey(key)){
                adAndClickTime.put(key, adAndClickTime.get(key)+input.f1);
            } else{
                adAndClickTime.put(key, input.f1);
            }
        }

        adAndClickTime.forEach(
                (xtime, yclick) -> {
                    String jsonElem = "{\"xtime\":\""+xtime+"\",\"yclick\":\""+yclick+"\"},";
                    System.out.println(jsonElem);
                }
        );
    }
}