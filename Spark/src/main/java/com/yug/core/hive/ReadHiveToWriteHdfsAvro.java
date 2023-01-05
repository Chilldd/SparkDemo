package com.yug.core.hive;

import com.yug.constants.Constants;
import com.yug.utils.SparkUtils;
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
public class ReadHiveToWriteHdfsAvro {
    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "root");
        SparkSession spark = SparkUtils.buildHiveSparkSession("Read Hive Data To HDFS Avro File");

        // 执行HiveSQL，将查询结果转换为Spark表
        spark.sql("select * from ods_di_asddd;")
                .createOrReplaceTempView("ods_asddd");
        // 查询Spark表
        Dataset<Row> dataset = spark.sql("select * from ods_asddd")
                .limit(10);
        // 查询结果写入HDFS中，格式为Avro
        dataset.select("id", "time_stamp", "fi_createtime", "fi_updatetime")
                .write()
                .format("avro")
                .save(
                        String.format("hdfs://%s/spark/avro/ods_asddd/%s",
                                Constants.HDFS_ADDRESS,
                                UUID.randomUUID().toString())
                );
        spark.stop();
    }
}
