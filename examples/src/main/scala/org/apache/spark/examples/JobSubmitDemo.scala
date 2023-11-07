package org.apache.spark.examples


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object JobSubmitDemo {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
                              .builder().master("local[1]")
                              .appName("jobsubmitdemo")
                              .getOrCreate()

    val df: DataFrame = spark.read.text(
      "/Users/alanzhao/Desktop/bigdata_code/spark/examples/src/main/resources/kv1.txt")
    val rdd1: RDD[(Row, Int)] = df.rdd.map((_, 1))

//
//    val value: RDD[(Row, Int)] = rdd1.reduceByKey(_ + _)
//
//    val rdd3: RDD[(Row, Int)] = value.sortBy(_._1)
//    println(rdd3.count())
     df.show(10)


  }

}
