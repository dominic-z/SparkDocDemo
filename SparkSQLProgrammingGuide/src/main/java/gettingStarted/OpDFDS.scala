package gettingStarted


import cases.{Person, Student, StudentScore}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
 * @author dominiczhu
 * @date 2020/8/26 7:27 下午
 */
object OpDFDS {
  implicit val sparkConf = new SparkConf().setAppName("local").setMaster("local[2]")
  implicit val sc = new SparkContext(sparkConf)
  implicit val spark = SparkSession.builder().config(sparkConf).getOrCreate()

  import spark.implicits._

  def main(args: Array[String]): Unit = {

//    createDsWithTuple
    filterDemo
  }

  def createDsWithTuple(implicit spark: SparkSession, sc: SparkContext): Unit = {


    val rdd = sc.parallelize(Seq((Person("person", 12L), Student("student", 12)))) //这种情况里表头是_1,_2，因为传入的是tuple

    rdd.toDS().show()


  }

  def opDfWithNestedCase(implicit spark: SparkSession, sc: SparkContext): Unit = {
    val studentScoreDs = Seq(StudentScore(Student("mike", 19), 89), StudentScore(Student("tom", 20), 87)).toDS()


  }

  def filterDemo(implicit spark: SparkSession, sc: SparkContext)={
    val ds=sc.parallelize(Seq(Student("s1", 12), Student("s2", 12),Student("s3", 12))).toDS()
    val res=ds.filter("stuName in ('s1','s2')").filter("stuName='s2'")
    res.show()
  }

}
