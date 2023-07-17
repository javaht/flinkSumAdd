import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.postgres.PostgreSQLSource;
import entity.Datashow;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.text.SimpleDateFormat;


public class dataStatistics {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2设置并行度
        env.setParallelism(1);//设置并行度为1方便测试
        //TODO 2.检查点配置
        //2.1 开启检查点
        env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE);//5秒执行一次，模式：精准一次性
        //2.2 设置检查点超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000);
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


        // 数据库的表有些没有时间字段
        ProcessingTimeSessionWindows processingSecondsMinuteWindows = ProcessingTimeSessionWindows.withGap(Time.minutes(1));
        ProcessingTimeSessionWindows processingHourSessionWindows = ProcessingTimeSessionWindows.withGap(Time.hours(1));
        ProcessingTimeSessionWindows processingDaysSessionWindows = ProcessingTimeSessionWindows.withGap(Time.days(1));

        //x->代表所有的数据都会在一个窗口。
        WindowedStream<String, String, TimeWindow> windowMinute = filterDs.keyBy(x -> "true").window(processingSecondsMinuteWindows);

        WindowedStream<String, String, TimeWindow> windowHours = filterDs.keyBy(x -> "true").window(processingHourSessionWindows);

        WindowedStream<String, String, TimeWindow> windowDays= filterDs.keyBy(x -> "true").window(processingDaysSessionWindows);



        //每十秒新增的数据
        SingleOutputStreamOperator<Datashow> resultMinute= windowMinute.aggregate(new MyAgg(), new MyProcess());
        //每24小时新增的数据
        SingleOutputStreamOperator<Datashow> resultHours = windowHours.aggregate(new MyAgg(), new MyProcess());
        //每24小时新增的数据
        SingleOutputStreamOperator<Datashow> resultDays = windowDays.aggregate(new MyAgg(), new MyProcess());


        SinkFunction<Datashow> sink = JdbcSink.sink(
                "insert into zhanshi.datashow (`databases`, windowStart, windowEnd,count,systemTime,sinkType) values (?, ?, ?, ?, ?,?)",
                new JdbcStatementBuilder<Datashow>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, Datashow datashow) throws SQLException {
                        preparedStatement.setString(1, datashow.getDatabases());
                        preparedStatement.setString(2, datashow.getWindowStart());
                        preparedStatement.setString(3, datashow.getWindowEnd());
                        preparedStatement.setInt(4, datashow.getCount());
                        preparedStatement.setString(5, datashow.getSystemTime());
                        preparedStatement.setString(6,datashow.getSinkType());
                    }
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(1000)
                        .withBatchIntervalMs(200)
                        .withMaxRetries(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://127.0.0.1:3306/zhanshi")
                        .withDriverName("com.mysql.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("123456")
                        .build()
        );



        resultMinute.addSink(sink);
        resultHours.addSink(sink);
        resultDays.addSink(sink);

        //执行
        env.execute();
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
                    .databases("cdctest")
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
