package com.yug.core.hdfs;

import com.yug.constants.Constants;
import com.yug.utils.SparkUtils;
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
        SparkSession spark = SparkUtils.buildDefaultSparkSession("Read HDFS Avro File");

        spark.read()
                .format("avro")
                .load(String.format("hdfs://%s/spark/avro/ods_asddd/*", Constants.HDFS_ADDRESS))
                .createOrReplaceTempView("ods_asddd");
        spark.sql("select count(1) as counts from ods_asddd")
                .show();

        spark.stop();
    }
}
