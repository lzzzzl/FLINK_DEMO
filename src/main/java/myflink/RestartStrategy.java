package myflink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;


public class RestartStrategy {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 每隔1000 ms进行启动一个检查点【设置checkpoint的周期】
        env.enableCheckpointing(1000);

        // 间隔1分钟 重启3次
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart( 3,// 尝试重启的次数
                Time.of(1, TimeUnit.MINUTES)));

        //5分钟内若失败了3次则认为该job失败，重试间隔为10s
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3,
                Time.of(5,TimeUnit.MINUTES),Time.of(10,TimeUnit.SECONDS)));

        //不重试
        env.setRestartStrategy(RestartStrategies.noRestart());

    }

}