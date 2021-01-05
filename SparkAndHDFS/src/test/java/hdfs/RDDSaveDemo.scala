package hdfs

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.junit.Test

/**
 * @author dominiczhu
 * @date 2020/11/21 下午3:48
 */
class RDDSaveDemo {
  val sparkConf: SparkConf = new SparkConf().setAppName("local").setMaster("local[2]")
  val sc: SparkContext = new SparkContext(sparkConf)
  val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

  @Test
  def textFileEmptyDir(): Unit = {
    //    如果目标文件夹已经存在，那么就会报错
    val dir = "fake_hdfs/saveAsText"
    val rdd = sc.parallelize(Array.range(0, 10))
    rdd.saveAsTextFile(dir)
  }

  @Test
  def saveEmptyRDD(): Unit = {
    //    如果目标文件夹已经存在，那么就会报错
    val dir = "fake_hdfs/save_empty_rdd_as_text"
    val rdd = sc.parallelize(Array.range(0, 10)).filter(_ < 0)
    rdd.saveAsTextFile(dir)
  }


  @Test
  def saveEmptyRddAsTextFile(): Unit = {
    //    如果目标文件夹已经存在，那么就会报错
    val dir = "fake_hdfs/saveEmptyRddAsText"
    val rdd = sc.textFile("fake_hdfs/empty.txt")
    rdd.saveAsTextFile(dir)
  }
}
