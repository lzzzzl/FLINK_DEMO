package myflink;


import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by 59702 on 2020/4/9.
 */
public class DisCache {

    public static void main(String[] args) throws Exception {
        //获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //1：注册一个文件,可以使用hdfs上的文件 也可以是本地文件进行测试
        //text 中有4个单词:hello flink hello FLINK
        env.registerCachedFile("/home/zzl/opt/flink-1.7.2/file/a.txt","a.txt");

        DataSource<String> data = env.fromElements("a", "b", "c", "d");

        DataSet<String> result = data.map(new RichMapFunction<String, String>() {
            private ArrayList<String> dataList = new ArrayList<String>();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                // 使用文件
                File file = getRuntimeContext().getDistributedCache().getFile("a.txt");
                System.err.println("分布式缓存为：" + file.getPath());
                List<String> lines = FileUtils.readLines(file);
                for (String line : lines) {
                    this.dataList.add(line);
                    System.err.println("分布式缓存为：" + line);
                }
            }

            @Override
            public String map(String value) throws Exception {
                System.err.println("使用datalist：" + dataList + "------------" +value);
                return dataList +"：" +  value;
            }
        });

        result.printToErr();


    }

}
