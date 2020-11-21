package hdfs

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.junit.Test

/**
 * @author dominiczhu
 * @date 2020/11/21 下午12:33
 */
class ReadFile {
  val sparkConf: SparkConf = new SparkConf().setAppName("local").setMaster("local[2]")
  val sc: SparkContext = new SparkContext(sparkConf)
  val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

  @Test
  def readEmptyDir(): Unit = {
    //    如果sc来读取文件没有match上，会报错
    val emptyDirPath="hdfs://xxx"
    val rdd=sc.textFile(s"${emptyDirPath}/*")
    println("read empty dir")
    println(rdd.collect().mkString("\n"))
  }
}
