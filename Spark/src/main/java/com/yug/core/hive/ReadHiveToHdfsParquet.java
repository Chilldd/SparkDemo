package com.yug.core.hive;

import com.yug.constants.Constants;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.collection.Seq;

import java.util.UUID;

/**
 * @author gy
 * @version 1.0
 * @description 连接Hive
 * @date 2022/12/15 15:23
 */

public class ReadHiveToHdfsParquet {
    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "root");

        SparkConf conf = new SparkConf()
                .setAppName("Hive SQL")
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
        // 查询结果写入HDFS中，格式为Parquet
        dataset.write()
                .parquet(String.format("hdfs://%s/spark/parquet/ods_asddd/%s", Constants.HDFS_ADDRESS, UUID.randomUUID().toString()));
        spark.stop();
    }
}
