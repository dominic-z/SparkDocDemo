import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object RDDDemo {
  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf().setAppName("local").setMaster("local[2]")
    val sc=new SparkContext(sparkConf)
    val spark=SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    val data = Array(1, 2, 3, 4, 5)
    val distData = sc.parallelize(data)

    val distFile = sc.textFile("demo.txt")
    val lineLengths = distFile.map(line=>line.split(",").length)
    print(lineLengths)
    val totalLines=lineLengths.reduce((a,b)=>a+b)
    print(totalLines)

    val pairs = distFile.flatMap(line=>line.split(",")).map(s => (s, 1))
    val counts = pairs.reduceByKey((a, b) => a + b)
    println(counts.collect().mkString(","))

    val distPartitions=distData.mapPartitions[Int](_=>{Iterator(1,2,3)})
    println(distPartitions.collect().mkString(","))


  }
}
