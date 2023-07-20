package windowSum;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.postgres.PostgreSQLSource;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import windowSum.MyDeserializationSchemaFunction;

//这个是开启的全窗口来统计的
public class processWindow {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //1.2设置并行度
        env.setParallelism(1);//设置并行度为1方便测试
        //TODO 2.检查点配置
        //2.1 开启检查点
        env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE);//5秒执行一次，模式：精准一次性
        //2.2 设置检查点超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000);
        env.getCheckpointConfig().setCheckpointStorage("file:///D:/tmp");
        //2.3 设置重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, 2 * 1000));//两次，两秒执行一次
        //2.4 设置job取消后检查点是否保留
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);//保留
        //2.5 设置状态后端-->保存到hdfs
        // env.setStateBackend(new FsStateBackend("hdfs://192.168.231.121:8020/ck"));
        //2.6 指定操作hdfs的用户
        System.setProperty("HADOOP_USER_NAME", "hive");


//TODO 3.FlinkCDC
        //3.1 创建MySQLSource
        SourceFunction<String> sourceFunction = PostgreSQLSource.<String>builder()
                .hostname("127.0.0.1")
                .port(5432)
                .database("cdctest")
                .schemaList("public")
                .tableList("public.*")
                .slotName("mytest")
                .decodingPluginName("pgoutput")
                .username("postgres")
                .password("123456")
                .deserializer(new MyDeserializationSchemaFunction())
                .build();

        //3.2 从源端获取数据
        DataStreamSource<String> alldata = env.addSource(sourceFunction);


        SingleOutputStreamOperator<String> filterDs = alldata.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String jsonstr) throws Exception {
                String type = JSONObject.parseObject(jsonstr).getString("type");
                if (type.equals("read")) {
                    return true;
                }
                return false;
            }
        });


        // 定义8小时、1天、1周、1月滚动窗口

        ProcessingTimeSessionWindows processingTimeSessionWindows = ProcessingTimeSessionWindows.withGap(Time.seconds(5));

//        TumblingEventTimeWindows tumblingSecondsWindows = TumblingEventTimeWindows.of(Time.seconds(10));
//        TumblingEventTimeWindows tumblingMinutesWindows = TumblingEventTimeWindows.of(Time.minutes(1));
//        TumblingEventTimeWindows tumblingHourWindows = TumblingEventTimeWindows.of(Time.hours(1));
//        TumblingEventTimeWindows tumblingDayWindows = TumblingEventTimeWindows.of(Time.days(1));
//        TumblingEventTimeWindows tumblingWeekWindows = TumblingEventTimeWindows.of(Time.days(7));
//        TumblingEventTimeWindows tumblingMonthWindows = TumblingEventTimeWindows.of(Time.days(30));

         WindowedStream<String, String, TimeWindow> window = filterDs.keyBy(x -> "true").window(processingTimeSessionWindows);

         window.process(new ProcessWindowFunction<String, String, String, TimeWindow>() {
             @Override
             public void process(String s, Context context, Iterable<String> iterable, Collector<String> collector) throws Exception {
                 System.out.println("s是什么："+s);
                 long l = iterable.spliterator().estimateSize();
                 collector.collect("窗口的大小是："+l );
             }
         }).print();


        //执行
        env.execute();
    }



}

