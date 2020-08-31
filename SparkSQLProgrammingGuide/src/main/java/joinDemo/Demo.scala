package joinDemo

import cases.{Person, Student, StudentScore}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
 * @author dominiczhu
 * @date 2020/8/26 3:49 下午
 */
object Demo {
  val sparkConf = new SparkConf().setAppName("local").setMaster("local[2]")
  implicit val sc = new SparkContext(sparkConf)
  implicit val spark = SparkSession.builder().config(sparkConf).getOrCreate()

  import spark.implicits._

  def main(args: Array[String]): Unit = {

    simpleJoin
    //    casesDfJoin
    //    nestedCasesDfAndSimpleDfJoin
  }

  def simpleJoin(implicit spark: SparkSession, sc: SparkContext): Unit = {


    val leftDS = Seq(Person("Andy", 32), Person("Mike", 32)).toDS()

    val rightDS2 = Seq(Student("Andy", 32)).toDS()

    val resDf = leftDS.join(rightDS2, $"name" <=> $"stuName" && $"age" <=> $"stuAge")
    resDf.show()


    var joinWithTheSameName = leftDS.join(leftDS, leftDS("name")===leftDS("name"))
    joinWithTheSameName.show()

    joinWithTheSameName = leftDS.as("left").join(leftDS.as("right"), $"left.name".as("leftName") <=> $"right.name")
    joinWithTheSameName.show() //不好使

    //    joinWithTheSameName.select("name").show()// 会报错的，因为这里有多个列都叫name 解决方法见博客
    joinWithTheSameName.select($"left.name",$"right.name".as("rightName"),$"right.age").show()//好使

    joinWithTheSameName = leftDS.as("left").join(leftDS.as("right"), Seq("name"))
    joinWithTheSameName.show() //只剩下一个name了，但还是会有两个age
  }

  def casesDfJoin(implicit spark: SparkSession, sc: SparkContext): Unit = {


    val leftDS = Seq(StudentScore(Student("mike", 19), 89), StudentScore(Student("amy", 18), 85)).toDS()
    val rightDS = Seq(StudentScore(Student("mike", 19), 89), StudentScore(Student("tom", 20), 87)).toDS()

    leftDS.show()
    rightDS.show()
    val resDf = leftDS.join(rightDS, Seq("student", "score"))
    resDf.show()
  }

  def nestedCasesDfAndSimpleDfJoin(implicit spark: SparkSession, sc: SparkContext): Unit = {
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


