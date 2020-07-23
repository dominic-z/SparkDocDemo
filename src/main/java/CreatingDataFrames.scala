import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

object CreatingDataFrames {
  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf().setAppName("local").setMaster("local[2]")
    val sc=new SparkContext(sparkConf)
    val spark=SparkSession.builder().config(sparkConf).getOrCreate()

    val df = spark.read.json("people.json")

    // Displays the content of the DataFrame to stdout
    df.show()
  }


}
