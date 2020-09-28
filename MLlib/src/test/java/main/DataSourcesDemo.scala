package main

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.junit.Test

/**
 * @author dominiczhu
 * @date 2020/9/28 2:21 下午
 */
class DataSourcesDemo {
  val sparkConf = new SparkConf().setAppName("local").setMaster("local[5]")
  val sc = new SparkContext(sparkConf)
  val spark = SparkSession.builder().config(sparkConf).getOrCreate()
  import spark.implicits._

  @Test
  def testImageDataSource():Unit={
    val df = spark.read.format("image").option("dropInvalid", true).load("data/mllib/images/origin/kittens")
    println(df)
    df.select("image.origin", "image.width", "image.height").show(truncate=false)
  }

  @Test
  def testLibSvm():Unit={
    val df = spark.read.format("libsvm").option("numFeatures", "780").load("data/mllib/sample_libsvm_data.txt")
    println(df)
    df.show(10)
  }
}
