package hdfs

import java.nio.charset.StandardCharsets
import java.util.Date

import org.apache.{commons, hadoop}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.junit.Test

/**
 * @author dominiczhu
 * @date 2020/11/21 上午10:18
 */
class AppendFile {
  val sparkConf: SparkConf = new SparkConf().setAppName("local").setMaster("local[2]")
  val sc: SparkContext = new SparkContext(sparkConf)
  val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

  @Test
  def append(): Unit = {
    val logFile = new Path("hdfs://xxx")
    val fs = FileSystem.get(logFile.toUri, sc.hadoopConfiguration)
    if (!fs.exists(logFile))
      fs.create(logFile).close()

    val inputStream = commons.io.IOUtils.toInputStream(s"${new Date()}, aaaaa\n", StandardCharsets.UTF_8)
    val outputStream = fs.append(logFile)
    hadoop.io.IOUtils.copyBytes(inputStream, outputStream, 4096, true)
  }
}
