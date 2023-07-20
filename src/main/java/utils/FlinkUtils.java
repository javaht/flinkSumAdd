package utils;

import entity.Datashow;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public  class FlinkUtils {

    public  static  StreamExecutionEnvironment  initEnv() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2设置并行度
        env.setParallelism(1);//设置并行度为1方便测试
        //TODO 2.检查点配置
        //2.1 开启检查点
        env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE);//5秒执行一次，模式：精准一次性
        //2.2 设置检查点超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000);
        //设置checkpoint的位置
        env.getCheckpointConfig().setCheckpointStorage("hdfs://nameservice1/flink/sumdata");
        //2.3 设置重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, 2 * 1000));//两次，两秒执行一次
        //2.4 设置job取消后检查点是否保留
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);//保留
        //2.5 设置状态后端-->保存到hdfs
        // env.setStateBackend(new FsStateBackend("hdfs://192.168.231.121:8020/ck"));
        //2.6 指定操作hdfs的用户
        System.setProperty("HADOOP_USER_NAME", "hive");

        return env;
    }


    public static SinkFunction<Datashow> sink(){

        SinkFunction<Datashow> sink = JdbcSink.sink(
                "insert into dataease.datashow (`databases`, windowStart, windowEnd,count,systemTime,sinkType) values (?, ?, ?, ?, ?,?)",
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
                        .withUrl("jdbc:mysql://1xxx:3306/dataease")
                        .withDriverName("com.mysql.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("x.x")
                        .build()
        );
        return  sink;
    }




}
