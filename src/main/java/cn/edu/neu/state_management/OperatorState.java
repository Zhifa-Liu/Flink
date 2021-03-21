package cn.edu.neu.state_management;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

/**
 * @author 32098
 *
 * Desc requirement:
 * 使用OperatorState支持的数据结构ListState存储offset信息, 模拟Kafka的offset维护,
 * 其实就是FlinkKafkaConsumer底层对应offset的维护!
 *
 * wordcount：env.fromElement(FromElementFunction) operator state 的使用
 */
public class OperatorState {

    public static void main(String[] args) throws Exception {
        String path = System.getProperty("user.dir");

        // 1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 设置Checkpoint时间间隔和磁盘路径以及代码遇到异常后的重启策略
        // 每隔 1s 执行一次Checkpoint
        env.enableCheckpointing(1000);
        env.setStateBackend(new FsStateBackend("file:///"+path+"/data/checkpoint"));
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 固定延迟重启策略: 程序出现异常的时候，重启2次，每次延迟3秒钟重启，超过2次，程序退出
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, 3000));

        //2.Source
        DataStreamSource<String> sourceData = env.addSource(new MyKafkaSource());

        //3.Transformation
        //4.Sink
        sourceData.print();

        //5.execute
        env.execute();
    }

    /**
     * MyKafkaSource就是模拟的FlinkKafkaConsumer并维护offset
     */
    public static class MyKafkaSource extends RichParallelSourceFunction<String> implements CheckpointedFunction {
        // -a.声明一个OperatorState来记录offset
        private ListState<Long> offsetState = null;
        private Long offset = 0L;
        private boolean flag = true;

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            // -b.创建状态描述器
            ListStateDescriptor<Long> descriptor = new ListStateDescriptor<Long>("offsetState", Long.class);
            // -c.根据状态描述器初始化状态
            offsetState = context.getOperatorStateStore().getListState(descriptor);
        }

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            // -d.获取并使用State中的值
            Iterator<Long> iterator = offsetState.get().iterator();
            if (iterator.hasNext()){
                offset = iterator.next();
            }
            while (flag){
                offset += 1;
                int id = getRuntimeContext().getIndexOfThisSubtask();
                // 1 2 3 4 5 6
                ctx.collect("分区" + id + "消费到的offset位置为:" + offset);
                // Thread.sleep(1000);
                TimeUnit.SECONDS.sleep(2);
                if(offset % 5 == 0){
                    System.out.println("程序遇到异常了.....");
                    throw new Exception("程序遇到异常了.....");
                }
            }
        }

        @Override
        public void cancel() {
            flag = false;
        }

        /**
         * 下面的snapshotState方法会按照固定的时间间隔将State信息存储到Checkpoint/磁盘中,也就是在磁盘做快照!
         */
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            // -e.保存State到Checkpoint中
            // 清理内存中存储的offset到Checkpoint中
            offsetState.clear();
            // -f.将offset存入State中
            offsetState.add(offset);
        }
    }
}
