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
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Date;

public class dataStatistics {
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
                .hostname("192.168.20.66")
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


//      alldata.filter(x -> JSONObject.parseObject(x).get("type").equals("insert")).print();

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
        ProcessingTimeSessionWindows processingSecondsSessionWindows = ProcessingTimeSessionWindows.withGap(Time.seconds(10));
        ProcessingTimeSessionWindows processingMinusSessionWindows = ProcessingTimeSessionWindows.withGap(Time.minutes(1));
        ProcessingTimeSessionWindows processingHourSessionWindows = ProcessingTimeSessionWindows.withGap(Time.hours(10));
        ProcessingTimeSessionWindows processingDaysSessionWindows = ProcessingTimeSessionWindows.withGap(Time.days(7));

        //x->代表所有的数据都会在一个窗口。
         WindowedStream<String, String, TimeWindow> window = filterDs.keyBy(x -> "true").window(processingSecondsSessionWindows);

        SingleOutputStreamOperator<Datashow> resultDs = window.aggregate(new MyAgg(), new MyProcess());
        resultDs.print();

        resultDs.addSink(JdbcSink.sink(
                "insert into datashow (`database`, windowStart, windowEnd,count,systemTime) values (?, ?, ?, ?, ?)",
                new JdbcStatementBuilder<Datashow>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, Datashow datashow) throws SQLException {
                        preparedStatement.setString(1, datashow.getDatabase());
                        preparedStatement.setString(2, datashow.getWindowStart());
                        preparedStatement.setString(3, datashow.getWindowEnd());
                        preparedStatement.setInt(4, datashow.getCount());
                        preparedStatement.setDate(5, datashow.getSystemTime());
                    }
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(1000)
                        .withBatchIntervalMs(200)
                        .withMaxRetries(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://192.168.20.66:3306/zhanshi")
                        .withDriverName("com.mysql.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("123456")
                        .build()
        ));

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
        int count = Integer.parseInt(elements.toString().replace("[","").replace("]",""));
        Date dateTime = new Date(System.currentTimeMillis());
        Datashow datashow = Datashow.builder().database("cdctest").windowStart(windowStart).windowEnd(windowEnd).count(count).systemTime(dateTime).build();
        collector.collect(datashow);
    }

}

}

