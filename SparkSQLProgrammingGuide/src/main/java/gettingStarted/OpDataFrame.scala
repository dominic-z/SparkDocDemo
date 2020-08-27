package gettingStarted

import joinDemo.Student
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
 * @author dominiczhu
 * @date 2020/8/26 7:27 下午
 */
object OpDataFrame {
  def main(args: Array[String]): Unit = {
    implicit val sparkConf=new SparkConf().setAppName("local").setMaster("local[2]")
    implicit val sc=new SparkContext(sparkConf)
    implicit val spark=SparkSession.builder().config(sparkConf).getOrCreate()
    createDf
  }

  def createDf(implicit spark:SparkSession,sc:SparkContext): Unit ={
    import spark.implicits._

    val rdd=sc.parallelize(Seq((Person("person",12L),Student("student",12))))
    rdd.toDF().show()
  }
}
