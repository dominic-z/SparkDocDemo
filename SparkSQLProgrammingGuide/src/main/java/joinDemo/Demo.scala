package joinDemo

import gettingStarted.Person
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
 * @author dominiczhu
 * @date 2020/8/26 3:49 下午
 */
object Demo {
  def main(args: Array[String]): Unit = {
    simpleJoin
  }

  def simpleJoin(): Unit = {
    val sparkConf = new SparkConf().setAppName("local").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    val leftDS = Seq(Person("Andy", 32), Person("Mike", 32)).toDS()

    val rightDS2 = Seq(Student("Andy", 32)).toDS()

    val resDf = leftDS.join(rightDS2, $"name"<=>$"stuName"&&$"age"<=>$"stuAge")
    resDf.show()

  }

}

case class Student(stuName:String,stuAge:Int)
