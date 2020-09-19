package datasetDemo

import cases.{Person, Student, StudentScore}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.junit.Test

/**
 * @author dominiczhu
 * @date 2020/8/26 3:49 下午
 */
class JoinDemo {
  val sparkConf: SparkConf = new SparkConf().setAppName("local").setMaster("local[2]")
  val sc: SparkContext = new SparkContext(sparkConf)
  val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

  import spark.implicits._

  def main(args: Array[String]): Unit = {

    simpleJoin
    //    casesDfJoin
    //    nestedCasesDfAndSimpleDfJoin
    //    nullJoin
  }

  /**
   * 演示最简单的join
   *
   */
  @Test
  def simpleJoin(): Unit = {

    val leftDS = Seq(Person("Andy", 32), Person("Mike", 32)).toDS()

    val rightDS = Seq(Student("Andy", 32)).toDS()

    //    多列进行join
    val resDf = leftDS.join(rightDS, $"name" <=> $"stuName" && $"age" <=> $"stuAge")
    resDf.show()

    //    下面是如果列名相同的情况下进行join，通过自己join来演示，这种情况会出现两个name列
    var joinWithTheSameName = leftDS.join(leftDS, leftDS("name") === leftDS("name"))
    joinWithTheSameName.show()

    //    下面这种情况会出现两个name列
    joinWithTheSameName = leftDS.as("left").join(leftDS.as("right"), $"left.name".as("leftName") <=> $"right.name")
    joinWithTheSameName.show() //不好使，结果的列头仍然是两个name两个age

    //    joinWithTheSameName.select("name").show()// 会报错的，因为这里有多个列都叫name 解决方法见博客，修改方法见下一行，加一个left就好
    joinWithTheSameName.select($"left.name", $"right.name".as("rightName"), $"right.age").show() //好使，说明这个ds里是可以区分两个name的

    joinWithTheSameName = leftDS.as("left").join(leftDS.as("right"), Seq("name"))
    joinWithTheSameName.show() //只剩下一个name了，但还是会有两个age，但这个不重要
  }

  /**
   * 演示如果有列为null的情况下会不会join成功
   *
   */
  @Test
  def nullJoin(): Unit = {
    val leftDS = Seq(Person("Andy", 32), Person("Mike", 32), Person(null, 32)).toDS()

    val rightDS = Seq(Student("Andy", 32), Student(null, 35)).toDS()
    val resDf = leftDS.join(rightDS, $"name" <=> $"stuName")
    resDf.show()
  }

  @Test
  def casesDfJoin(): Unit = {


    val leftDS = Seq(StudentScore(Student("mike", 19), 89), StudentScore(Student("amy", 18), 85)).toDS()
    val rightDS = Seq(StudentScore(Student("mike", 19), 89), StudentScore(Student("tom", 20), 87)).toDS()

    leftDS.show()
    rightDS.show()
    val resDf = leftDS.join(rightDS, Seq("student", "score"))
    resDf.show()
  }

  @Test
  def nestedCasesDfAndSimpleDfJoin(): Unit = {
    val leftDS = Seq(StudentScore(Student("mike", 19), 89), StudentScore(Student("amy", 18), 85)).toDS()
    leftDS.show()
    val rightRdd = sc.parallelize(Seq(("mike", 19, 95))) // tuple

    val rightDs = spark.createDataset(rightRdd)
    rightDs.show()
    val rightDf = rightDs.withColumnRenamed("_1", "right_name").withColumnRenamed("_2", "right_age").withColumnRenamed("_3", "right_score")
    rightDf.show()

    //

    //    leftDS.show()
    //    rightDS.show()
    //    val resDf = leftDS.join(rightDS, Seq("student","score"))
    //    resDf.show()
  }
}


