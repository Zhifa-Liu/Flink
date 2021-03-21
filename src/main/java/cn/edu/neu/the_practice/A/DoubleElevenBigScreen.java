package cn.edu.neu.the_practice.A;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author 32098
 *
 *
 */
public class DoubleElevenBigScreen {
    /**
     * 用于存储聚合的结果
     */
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class CategoryPojo {
        // 分类名称
        private String category;
        // 该分类总销售额
        private double totalPrice;
        // 截止到当前时间的时间,本来应该是EventTime,但是我们这里简化了直接用当前系统时间即可
        private String dateTime;
    }

    /**
     * 自定义数据源实时产生订单模拟数据 Tuple2<分类, 金额>
     */
    public static class MySource implements SourceFunction<Tuple2<String, Double>>{
        private boolean flag = true;
        private final String[] categories = {"女装", "男装","图书", "家电","洗护", "美妆","运动", "游戏","户外", "家具","乐器", "办公"};
        private final Random random = new Random();

        @Override
        public void run(SourceContext<Tuple2<String, Double>> ctx) throws Exception {
            while (flag){
                // 随机生成分类和金额
                // [0~length) ==> [0~length-1]
                int index = random.nextInt(categories.length);
                // 随机分类
                String category = categories[index];
                // 随机金额：[0~100)
                double price = random.nextDouble() * 100;
                ctx.collect(Tuple2.of(category,price));
                Thread.sleep(20);
            }
        }

        @Override
        public void cancel() {
            flag = false;
        }
    }

    public static void main(String[] args) throws Exception{
        // 1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2.source
        // 模拟实时订单信息
        DataStreamSource<Tuple2<String, Double>> sourceDs = env.addSource(new MySource());

        /*
        注意:需求如下：
        -1.实时计算出11月11日00:00:00零点开始截止到当前时间的销售总额
        -2.计算出各分类销售额top3
        -3.每1秒钟更新一次统计结果
        如果使用之前学习的简单的timeWindow(Time size 窗口大小, Time slide 滑动间隔)来处理,
        如xxx.timeWindow(24小时,1s)，计算的是需求中的吗?
        不是!如果使用之前的做法那么是完成不了需求的，因为:
        如11月11日00:00:00计算的是11月10号[00:00:00~23:59:60s)的数据
        而我们应该要计算的是:11月11日00:00:00~11月11日00:00:01
        所以不能使用之前的简单做法!
        */

        // 3.transformation
        SingleOutputStreamOperator<CategoryPojo> tempAggResult = sourceDs.keyBy(0)
                // 3.1 定义大小为一天的窗口,第二个参数表示中国使用的UTC+08:00时区比UTC时间早
                /*
                of(Time 窗口大小, Time 带时间校准的从哪开始)源码中有解释:
                如果您居住在不使用UTC±00：00时间的地方，例如使用UTC + 08：00的中国，并且您需要一个大小为一天的时间窗口，
                并且窗口从当地时间的每00:00:00开始，您可以使用of(Time.days(1)，Time.hours(-8))
                注意:该代码如果在11月11日运行就会从11月11日00:00:00开始记录直到11月11日23:59:59的1天的数据
                注意:我们这里简化了没有把之前的 Watermaker 那些代码拿过来,所以直接ProcessingTime
                */
                // 仅仅只定义了一个窗口大小
                .window(TumblingProcessingTimeWindows.of(Time.days(1), Time.hours(-8)))
                // 3.2 定义一个 1s 的触发器
                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(1)))
                // 3.3 聚合结果：.aggregate(new PriceAggregate(), new WindowResult());
                // .sum(1) 以前的写法用的是默认的聚合和收集
                // 现在可以自定义如何对price进行聚合，并自定义聚合结果用怎样的格式进行收集
                .aggregate(new PriceAggregate(), new WindowResult());

        // 3.4 看一下初步聚合的结果
        tempAggResult.print("初步聚合结果>>>");
        /*
        CategoryPojo(category=运动, totalPrice=118.69, dateTime=2020-10-20 08:04:12)
        上面的结果表示:当前各个分类的销售总额
         */

        // 4.使用上面初步聚合的结果,实现业务需求,并sink
        // 按照时间分组是因为需要每1s更新截至到当前时间的销售总额
        tempAggResult.keyBy("dateTime")
                // 每秒钟更新一次统计结果
                // Time size 为 1s，表示计算最近1s的数据
                .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
                // 在ProcessWindowFunction中实现该复杂业务逻辑,一次性将需求1和2搞定
                // window后的process方法可以处理复杂逻辑
                .process(new WindowResultProcess());

        // 5.execute
        env.execute();
    }

    /**
     * 自定义价格聚合函数，其实就是对price的简单sum操作
     * AggregateFunction<IN, ACC, OUT>
     * AggregateFunction<Tuple2<String, Double>, Double, Double>
     *
     */
    private static class PriceAggregate implements AggregateFunction<Tuple2<String, Double>, Double, Double> {
        // 初始化累加器为0
        @Override
        public Double createAccumulator() {
            // D表示Double,L表示long
            return 0D;
        }

        // 把price往累加器上累加
        @Override
        public Double add(Tuple2<String, Double> value, Double accumulator) {
            return value.f1 + accumulator;
        }

        // 获取累加结果
        @Override
        public Double getResult(Double accumulator) {
            return accumulator;
        }

        // 各个subTask的结果合并
        @Override
        public Double merge(Double a, Double b) {
            return a + b;
        }
    }

    /**
     * 自定义WindowFunction，实现如何收集窗口结果数据
     * interface WindowFunction<IN, OUT, KEY, W extends Window>
     * interface WindowFunction<Double, CategoryPojo, Tuple的真实类型就是String就是分类, W extends Window>
     */
    private static class WindowResult implements WindowFunction<Double, CategoryPojo, Tuple, TimeWindow> {
        // 定义一个时间格式化工具用来将当前时间(双十一那天订单的时间)转为String格式
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        @Override
        public void apply(Tuple tuple, TimeWindow window, Iterable<Double> input, Collector<CategoryPojo> out) throws Exception {
            String category = ((Tuple1<String>) tuple).f0;

            Double price = input.iterator().next();
            // 为了后面项目铺垫,使用一下用 Bigdecimal 来表示精确的小数
            BigDecimal bigDecimal = new BigDecimal(price);

            // setScale设置精度保留2位小数，且设置四舍五入
            double roundPrice = bigDecimal.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();

            long currentTimeMillis = System.currentTimeMillis();
            String dateTime = df.format(currentTimeMillis);

            CategoryPojo categoryPojo = new CategoryPojo(category, roundPrice, dateTime);
            out.collect(categoryPojo);
        }
    }

    /**
     * 实现ProcessWindowFunction
     * abstract class ProcessWindowFunction<IN, OUT, KEY, W extends Window>
     * abstract class ProcessWindowFunction<CategoryPojo, Object, Tuple就是String类型的dateTime, TimeWindow extends Window>
     *
     * 把各个分类的总价加起来，就是全站的总销量金额，
     * 然后我们同时使用优先级队列计算出分类销售的Top3，
     * 最后打印出结果，在实际中我们可以把这个结果数据存储到 hbase 或者 redis 中，以供前端的实时页面展示。
     */
    private static class WindowResultProcess extends ProcessWindowFunction<CategoryPojo, Object, Tuple, TimeWindow> {
        @Override
        public void process(Tuple tuple, Context context, Iterable<CategoryPojo> elements, Collector<Object> out) throws Exception {
            String dateTime = ((Tuple1<String>)tuple).f0;
            // Java中的大小顶堆可以使用优先级队列来实现
            // https://blog.csdn.net/hefenglian/article/details/81807527
            // 注意:
            // 小顶堆用来计算:最大的topN
            // 大顶堆用来计算:最小的topN
            Queue<CategoryPojo> queue = new PriorityQueue<>(
                    //初始容量
                    3,
                    // 正常的排序，就是小的在前，大的在后，也就是c1>c2的时候返回1，也就是小顶堆
                    (c1, c2) -> c1.getTotalPrice() >= c2.getTotalPrice() ? 1 : -1
            );

            // 在这里我们要完成需求:
            // * -1.实时计算出11月11日00:00:00零点开始截止到当前时间的销售总额，其实就是把之前的初步聚合的price再累加!
            double totalPrice = 0D;
            double roundPrice = 0D;
            Iterator<CategoryPojo> iterator = elements.iterator();
            for (CategoryPojo element : elements) {
                // 某个分类的总销售额
                double price = element.totalPrice;
                totalPrice += price;
                BigDecimal bigDecimal = new BigDecimal(totalPrice);
                roundPrice = bigDecimal.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
                // * -2.计算出各个分类的销售额top3,其实就是对各个分类的price进行排序取前3
                // 注意:我们只需要top3,也就是只关注最大的前3个的顺序,剩下不管!所以不要使用全局排序,只需要做最大的前3的局部排序即可
                // 那么可以使用小顶堆,把小的放顶上
                // c:80
                // b:90
                // a:100
                // 那么来了一个数，和最顶上的比，如d，
                // if(d>顶上)，把顶上的去掉，把d放上去，再和b、a比较并排序，保证顶上是最小的
                // if(d<=顶上)，不用变
                if (queue.size() < 3) { // 小顶堆size<3,说明数不够,直接放入
                    queue.add(element);
                }else{ // 小顶堆size=3,说明,小顶堆满了,进来一个需要比较
                    // "取出"顶上的(不是移除)
                    CategoryPojo top = queue.peek();
                    if(element.totalPrice > top.totalPrice){
                        // queue.remove(top); //移除指定的元素
                        queue.poll(); //移除顶上的元素
                        queue.add(element);
                    }
                }
            }
            // * -3.每1秒钟更新一次统计结果,可以直接打印/sink，也可以收集完结果返回后再打印，
            //  但是我们这里一次性处理了需求1和2的两种结果,不好返回,所以直接输出!
            // 对queue中的数据逆序
            // 各个分类的销售额top3
            List<String> top3Result = queue.stream()
                    .sorted((c1, c2) -> c1.getTotalPrice() > c2.getTotalPrice() ? -1 : 1) //逆序
                    .map(c -> "(分类：" + c.getCategory() + " 销售总额：" + c.getTotalPrice() + ")")
                    .collect(Collectors.toList());
            System.out.println("时间 ： " + dateTime + "  总价 : " + roundPrice + " top3:\n" + StringUtils.join(top3Result, ",\n"));
            System.out.println("-------------");
        }
    }
}


