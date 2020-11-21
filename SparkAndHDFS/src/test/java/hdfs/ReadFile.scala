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
  def textFileEmptyDir(): Unit = {
    //    如果sc来读取文件没有文件match得上，match上，会报错
    val emptyDirPath="fake_hdfs/empty_dir"
    val rdd=sc.textFile(s"${emptyDirPath}/*")
    println("read empty dir")
    println(rdd.collect().mkString("\n"))
  }

  @Test
  def textFileEmptyDirButADir(): Unit = {
    //    只要没有text文件match，也会报错
    val emptyDirPath="fake_hdfs/empty_dir_but_a_dir"
    val rdd=sc.textFile(s"${emptyDirPath}/*")
    println("read empty dir")
    println(rdd.collect().mkString("\n"))
  }


  @Test
  def textFileWithADir(): Unit = {
    //    即使这个文件夹下面有一个文件夹，那textfile也会只会读取这个文件夹下面的文件。
    val emptyDirPath="fake_hdfs/text_file_and_dir"
    val rdd=sc.textFile(s"${emptyDirPath}/*")
    println("text_file_and_dir")
    println(rdd.collect().mkString("\n"))
  }
}
