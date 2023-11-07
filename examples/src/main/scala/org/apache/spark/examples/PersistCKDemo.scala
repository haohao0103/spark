package org.apache.spark.examples

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object PersistCKDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .master("local[*]")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    val rdd: RDD[String] = sc.parallelize(Array("a", "b", "c", "d", "e","f","g"),2)  // 创建一个RDD
    val rdd1 = rdd.map(x => (x,1))
    val rdd2 = rdd.map(x => (x,100))

    val rdd3: RDD[(String, Int)] = rdd1.union(rdd2).repartition(5)

    rdd3.count()
    rdd3.collect()

    val rdd4 = rdd3.filter(_._2 > 50)
    rdd4.count()

    val rdd5 = rdd3.filter(_._2 < 50)
    rdd5.count()

    rdd.repartition(5).count()

    Thread.sleep(Long.MaxValue)
  }
}
