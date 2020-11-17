package hdfs

import java.io.IOException

import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
 * @author dominiczhu
 * @date 2020/11/17 下午7:47
 */
object CreateFileDemo {
  implicit val sparkConf = new SparkConf().setAppName("local").setMaster("local[5]")
  implicit val sc = new SparkContext(sparkConf)
  implicit val spark = SparkSession.builder().config(sparkConf).getOrCreate()

  def createHdfsFile(): Unit = {
    val path = new Path("hdfs://xxx/xxx")
    val hdfs = FileSystem.get(path.toUri, new org.apache.hadoop.conf.Configuration())
    var outputStream:FSDataOutputStream=null
    try {
      outputStream=hdfs.create(path, false)
      outputStream.writeUTF("Hei there")
    } catch {
      case _: IOException =>
        println("file already exists")
    }finally {
      outputStream.close()
    }
  }
}
