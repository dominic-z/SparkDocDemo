package DSAndHdfs

import cases.Person
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

/**
 * @author dominiczhu
 * @date 2020/11/21 下午7:48
 */
class Writer {
  val sparkConf: SparkConf = new SparkConf().setAppName("local").setMaster("local[2]")
  val sc: SparkContext = new SparkContext(sparkConf)
  val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
  import spark.implicits._
  @Test
  def json(): Unit = {
    val ds = Seq(Person("Andy", 32), Person("Mike", 32)).toDS()
    ds.write.json("data/output/output.json")
  }
}
