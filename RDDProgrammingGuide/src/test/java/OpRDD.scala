import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.junit.Test

/**
 * @author dominiczhu
 * @date 2020/11/19 下午2:29
 */
class OpRDD {

  val sparkConf: SparkConf = new SparkConf().setAppName("local").setMaster("local[2]")
  val sc: SparkContext = new SparkContext(sparkConf)
  val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

  @Test
  def emptyRddReduceByKey(): Unit = {
    val data = Array((1,1), (2,2), (1,3))
    val rdd = sc.parallelize(data).filter(t=>t._1<0)
    println(rdd.collect().toSeq)

    println(rdd.reduceByKey((a,b)=>a+b).collect().toSeq)
  }

  @Test
  def unionEmptyRddArray():Unit={
//    不会报错
    val arr=Array[RDD[String]]()
    println(sc.union(arr).collect().mkString(","))
  }

  @Test
  def mapDemo():Unit={
    val data = Array((1,1), (2,2), (1,3))
    val rdd = sc.parallelize(data).map((_,1))
    println(rdd.collect().toSeq)
  }


}
