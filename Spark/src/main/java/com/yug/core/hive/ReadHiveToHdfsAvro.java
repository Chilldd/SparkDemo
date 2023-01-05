package com.yug.core.hive;

import com.yug.constants.Constants;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.UUID;

/**
 * @author gy
 * @version 1.0
 * @description TODO
 * @date 2023/1/3 16:15
 */
public class ReadHiveToHdfsAvro {
    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "root");
        System.setProperty("packages", "org.apache.spark:spark-avro_2.12:3.3.1");

        SparkConf conf = new SparkConf()
                .setAppName("Read Hive Data To HDFS Avro File")
                .setMaster(String.format("spark://%s", Constants.SPARK_ADDRESS))
                .set("spark.submit.deployMode", "client")
                .set("spark.executor.cores", "1")
                .set("spark.executor.memory", "1024M")
                .set("spark.driver.host", Constants.LOCALHOST_IP);

        SparkSession spark = SparkSession.builder()
                .config(conf)
                .enableHiveSupport()
                .getOrCreate();

        // 执行HiveSQL，将查询结果转换为Spark表
        spark.sql("select * from ods_di_asddd;")
                .createOrReplaceTempView("ods_asddd");
        // 查询Spark表
        Dataset<Row> dataset = spark.sql("select * from ods_asddd")
                .limit(10);
        // 查询结果写入HDFS中，格式为Avro
        dataset.write()
                .format("avro")
                .save(String.format("hdfs://%s/spark/avro/ods_asddd/%s", Constants.HDFS_ADDRESS, UUID.randomUUID().toString()));
        spark.stop();
    }
}
