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

    //    simpleJoin
//    casesDfJoin
    nestedCasesDfAndSimpleDfJoin
  }

  def simpleJoin(implicit spark: SparkSession, sc: SparkContext): Unit = {


    val leftDS = Seq(Person("Andy", 32), Person("Mike", 32)).toDS()

    val rightDS2 = Seq(Student("Andy", 32)).toDS()

    val resDf = leftDS.join(rightDS2, $"name" <=> $"stuName" && $"age" <=> $"stuAge")
    resDf.show()

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
    val rightDf=rightDs.withColumnRenamed("_1","right_name").withColumnRenamed("_2","right_age").withColumnRenamed("_3","right_score")
    rightDf.show()

    //

    //    leftDS.show()
    //    rightDS.show()
    //    val resDf = leftDS.join(rightDS, Seq("student","score"))
    //    resDf.show()
  }
}


