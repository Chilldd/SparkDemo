package com.yug.core.hdfs;

import com.yug.constants.Constants;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

/**
 * @author gy
 * @version 1.0
 * @description 读取HDFS上Parquet类型的文件
 * @date 2023/1/3 15:37
 */
public class ReadAvro {
    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "root");

        SparkConf conf = new SparkConf()
                .setAppName("Read HDFS Avro File")
                .setMaster(String.format("spark://%s", Constants.SPARK_ADDRESS))
                .set("spark.submit.deployMode", "client")
                .set("spark.executor.cores", "1")
                .set("spark.executor.memory", "1024M")
                .set("spark.driver.host", Constants.LOCALHOST_IP);

        SparkSession spark = SparkSession.builder()
                .config(conf)
                .enableHiveSupport()
                .getOrCreate();

        spark.read()
                .parquet(String.format("hdfs://%s/spark/avro/ods_asddd/*", Constants.HDFS_ADDRESS))
                .createOrReplaceTempView("ods_asddd");
        spark.sql("select * from ods_asddd")
                .show();

        spark.stop();
    }
}
