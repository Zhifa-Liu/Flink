package cn.edu.neu.experiment.advertise_click_count_nearly_minute;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author 32098
 */
public class JsonSink extends RichSinkFunction<Tuple2<String, Long>> {
    private TreeMap<String, Long> timeClick = null;
    private long lastInvokeTime = 0;
    private SimpleDateFormat dateFormat = null;


    @Override
    public void open(Configuration parameters) throws Exception {
        timeClick = new TreeMap<String, Long>();
        dateFormat = new SimpleDateFormat("ss");
        lastInvokeTime = System.currentTimeMillis();
    }

    @Override
    public void invoke(Tuple2<String, Long> value, Context context) throws Exception {
        long invokeTime = System.currentTimeMillis();
        if(Integer.parseInt(dateFormat.format(invokeTime)) - Integer.parseInt(dateFormat.format(lastInvokeTime))>1){
            writeToJson();
        }
        timeClick.put(value.f0, value.f1);
        lastInvokeTime = System.currentTimeMillis();
//        if(timeClick.containsKey(value.f0)){
//            return;
//        }
//        if(timeClick.size() == 6){
//            timeClick.pollFirstEntry();
//        }
//        timeClick.put(value.f0, value.f1);
//        writeToJson();
    }

    @Override
    public void close() throws Exception {
        // adAndClickTime.clear();
    }

    public void writeToJson(){
        String projectRoot = System.getProperty("user.dir");
        String file = projectRoot + "/src/main/java/cn/edu/neu/experiment/advertise_click_count_nearly_minute/advertise_click_count_nearly_minute.json";
        try {
            PrintWriter out = new PrintWriter(new FileWriter(new File(file), false));
            StringBuffer jsonStr = new StringBuffer("[");
            // System.out.println(timeClick.size());
            timeClick.forEach(
                    (time, click) -> {
                        String json = "{\"xtime\":\""+time+"\",\"yclick\":\""+click+"\"},";
                        jsonStr.append(json);
                        // System.out.println(json);
                    }
            );
            jsonStr.deleteCharAt(jsonStr.length()-1);
            jsonStr.append("]");
            out.println(jsonStr.toString());
            out.flush();
            out.close();
            timeClick.clear();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

