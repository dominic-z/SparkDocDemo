import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.junit.Test

import collection.mutable
import scala.util.Random

/**
 * @author dominiczhu
 * @date 2020/9/25 5:24 下午
 */
class ReduceDemo {
  val sparkConf: SparkConf = new SparkConf().setAppName("local").setMaster("local[2]")
  val sc = new SparkContext(sparkConf)
  val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()


  @Test
  def reduceByKey(): Unit = {
    val mutableList = for (i <- List.range(1, 11); j <- List.range(1, 4)) yield (s"id${i}", (Random.nextInt(20), j))

    val rdd = sc.parallelize(Random.shuffle(mutableList), 5)
    println(rdd.collect().toList)
    val res = rdd.reduceByKey((t1, t2) => {
      ((if (t1._2 == 2) -2 * t1._1 else t1._1) + (if (t2._2 == 2) -2 * t2._1 else t2._1), 0)
    })
    println(res.collect().toList)
  }
}
