package cn.edu.neu.batch_stream_api.sink;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author 32098
 *
 * ds.print(): to console
 * ds.printToErr(): to console with red color
 * ds.writeAsText("path", mode).setParallelism(i): if i = 1, path is file path, otherwise, path is dir path
 */
public class ConsoleFileBasedSink {
    public static void main(String[] args) {
        // 1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2.source
        DataStream<String> ds = env.readTextFile("data/words.txt");

        // 3.transformation
        // 4.sink
        ds.print();
        ds.printToErr();
        ds.writeAsText("data/output", FileSystem.WriteMode.OVERWRITE).setParallelism(2);

        //5.execute
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
