package windowSum;

import com.alibaba.fastjson.JSONObject;
import entity.Datashow;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import utils.FlinkUtils;
import utils.HandleUtils;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.text.SimpleDateFormat;


public class dataStatistics {

    public static void main(String[] args) throws Exception {

        String database = args[0]; //获取第一个参数
        // String database ="zxicetcyl_news";
        StreamExecutionEnvironment env = FlinkUtils.initEnv();

        SourceFunction<String> sourceFunction = HandleUtils.postgreSourceFunction("xxxx",
                15432,
                database,
                "public",
                "public.*",
                "dataease_"+database,
                "postgres",
                "xxxxx");


        SingleOutputStreamOperator<String> filterDs =  env.addSource(sourceFunction).filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String jsonstr) throws Exception {
                String type = JSONObject.parseObject(jsonstr).getString("type");
                if (type.equals("insert")) {
                    return true;
                }
                return false;
            }
        });

        // 数据库的表有些没有时间字段 设置为处理时间,窗口为滑动窗口
        TumblingProcessingTimeWindows  tumblingProcessingSecondsWindows = TumblingProcessingTimeWindows.of(Time.seconds(10));
        TumblingProcessingTimeWindows tumblingProcessingHoursWindows = TumblingProcessingTimeWindows.of(Time.hours(1));
        TumblingProcessingTimeWindows tumblingProcessingDayWindows = TumblingProcessingTimeWindows.of(Time.days(1));

        //x->代表所有的数据都会在一个窗口。
        WindowedStream<String, String, TimeWindow> windowMinute = filterDs.keyBy(x -> "true").window(tumblingProcessingSecondsWindows);
        WindowedStream<String, String, TimeWindow> windowHours = filterDs.keyBy(x -> "true").window(tumblingProcessingHoursWindows);
        WindowedStream<String, String, TimeWindow> windowDays= filterDs.keyBy(x -> "true").window(tumblingProcessingDayWindows);

        SingleOutputStreamOperator<Datashow> resultMinute= windowMinute.aggregate(new MyAgg(), new MyProcess(database));
        SingleOutputStreamOperator<Datashow> resultHours = windowHours.aggregate(new MyAgg(), new MyProcess(database));
        //这里指定了触发器
        SingleOutputStreamOperator<Datashow> resultDays = windowDays.trigger(new DailyTimeTrigger()).aggregate(new MyAgg(), new MyProcess(database));

        SinkFunction<Datashow> sink = FlinkUtils.sink();

        resultMinute.addSink(sink);
        resultHours.addSink(sink);
        resultDays.addSink(sink);

        //执行
        env.execute();
    }

    //触发器的应用
    public static class DailyTimeTrigger extends Trigger<Object, TimeWindow> {
        @Override
        public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx) {
            // 不做处理,等待触发时机
            return TriggerResult.CONTINUE;
        }
        // 判断时间戳是否为0点
        private boolean isMidnight(long time) {
            LocalDateTime localDateTime = LocalDateTime.ofEpochSecond(time / 1000, 0, ZoneOffset.UTC);
            // 2. 获取该时间的小时和分钟
            int hour = localDateTime.getHour();
            int minute = localDateTime.getMinute();
            // 3. 如果时0点则返回true
            if (hour == 0 && minute == 0) {
                return true;
            } else {
                return false;
            }
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) {
            // 判断当前时间是否达到0点
            if (isMidnight(time)) {
                return TriggerResult.FIRE_AND_PURGE;
            } else {
                return TriggerResult.CONTINUE;
            }
        }

        @Override
        public void clear(TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {

        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) {
            return TriggerResult.CONTINUE;
        }

    }

    public static class MyAgg implements AggregateFunction<String, Integer, String> {
        @Override
        public Integer createAccumulator() {
            System.out.println("创建累加器");
            return 0;
        }

        @Override
        public Integer add(String s, Integer accumulator) {
            return 1+accumulator;
        }

        @Override
        public String getResult(Integer accumulator) {
            return accumulator.toString();
        }

        @Override
        public Integer merge(Integer integer, Integer acc1) {
            System.out.println("调用merge方法");
            return null;
        }
    }

    public static class MyProcess extends ProcessWindowFunction<String,Datashow,String,TimeWindow> {

        private final String database;
        //构造器传递
        public MyProcess(String database) {
            this.database = database;
        }

        @Override
        public void process(String s, Context context, Iterable<String> elements, Collector<Datashow> collector) throws Exception {
            long startTs = context.window().getStart();
            long endTs = context.window().getEnd();

            String windowStart = DateFormatUtils.format(startTs, "yyyy-MM-dd HH:mm:ss.SSS");
            String windowEnd = DateFormatUtils.format(endTs, "yyyy-MM-dd HH:mm:ss.SSS");

            //新增条数
            int count = Integer.parseInt(elements.toString().replace("[","").replace("]",""));
            //获取系统当前时间
            String dateTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());

            //把毫秒变成秒
            long timeDiffSeconds  = (endTs-startTs)/1000;
            long timeDiffMinutes = timeDiffSeconds / 60;

            String sinkType="";

            if(timeDiffMinutes < 60){
                // 1小时内
                sinkType = "minute";

            }else if(timeDiffMinutes < 24 * 60){
                // 1天内
                sinkType = "hour";

            }else{
                sinkType = "day";
            }

            System.out.println("diffMinutes： "+timeDiffMinutes);

            Datashow datashow = Datashow.builder()
                    .databases(database)
                    .windowStart(windowStart)
                    .windowEnd(windowEnd)
                    .count(count)
                    .systemTime(dateTime)
                    .sinkType(sinkType)
                    .build();

            collector.collect(datashow);
        }

    }

}