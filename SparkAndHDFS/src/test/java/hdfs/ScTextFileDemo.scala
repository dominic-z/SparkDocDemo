package hdfs

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.junit.Test

/**
 * @author dominiczhu
 * @date 2020/11/23 下午5:03
 *//**
 *
 * @title: TextFileDemo
 * @Author Tan
 * @Date: 2020/11/23 下午5:03
 * @Version 1.0
 */
class ScTextFileDemo {
  val sparkConf: SparkConf = new SparkConf().setAppName("local").setMaster("local[2]")
  val sc: SparkContext = new SparkContext(sparkConf)
  val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

  @Test
  def textFile1(): Unit = {
    //    如果下面的读取行为，没有文件匹配，就会报个错，textFile接受通配符，但是必须有文件匹配得上
    val rdd1 = sc.textFile("fake_hdfs/for_textfile/*")
    println(rdd1.collect().mkString(";"))
  }

  @Test
  def textFile2(): Unit = {
    //    如果下面的读取行为，没有文件匹配，就会报个错，textFile接受通配符，但是必须有文件匹配得上
    //    操蛋的是_success文件不会被读取，下划线开头的都不会被textFile识别
    val rdd2 = sc.textFile("fake_hdfs/only_success/*")
    println(rdd2.collect().mkString(";"))

  }
}
