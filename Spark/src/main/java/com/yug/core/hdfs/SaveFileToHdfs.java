package com.yug.core.hdfs;

import com.yug.constants.Constants;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

/**
 * @author gy
 * @version 1.0
 * @description 保存文件到HDFS
 * @date 2022/12/14 16:10
 */
public class SaveFileToHdfs {
    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "root");

        SparkConf conf = new SparkConf()
                .setAppName("Simple Connection HDFS")
                .setMaster(String.format("spark://%s", Constants.SPARK_ADDRESS))
                .set("spark.submit.deployMode", "client")
                .set("spark.executor.cores", "1")
                .set("spark.executor.memory", "1024M")
                .set("spark.driver.host", Constants.LOCALHOST_IP);
        JavaSparkContext spark = new JavaSparkContext(conf);

        List<String> data = Arrays.asList("A", "B", "C", "D", "E", "F");
        JavaRDD<String> rdd = spark.parallelize(data, 1);
        String fileName = UUID.randomUUID().toString();
        rdd.saveAsTextFile(String.format("hdfs://%s/nifi/ods_di_asddd/%s", Constants.HDFS_ADDRESS, fileName));

        spark.stop();
    }
}
