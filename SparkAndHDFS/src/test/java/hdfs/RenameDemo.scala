package hdfs

import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.junit.Test

/**
 * @author dominiczhu
 * @date 2020/11/21 下午4:40
 */
class RenameDemo {
  val sparkConf: SparkConf = new SparkConf().setAppName("local").setMaster("local[2]")
  val sc: SparkContext = new SparkContext(sparkConf)
  val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

  //  本机之中，将文件挪到一个存在的文件夹下面会失败
//  但是在我公司的集群上，这么做会失败

  @Test
  def renameDir(): Unit = {
    val dirPath="fake_hdfs/dir_for_rename_src"
    val srcDir = new Path(dirPath)
    val hdfs = org.apache.hadoop.fs.FileSystem.get(srcDir.toUri,sc.hadoopConfiguration)
    println(s"success ${hdfs.rename(srcDir,new Path("fake_hdfs/dir_for_rename_dst"))}")
  }

  @Test
  def renameFile(): Unit = {
    val dirPath="fake_hdfs/file_for_rename_src"
    val srcDir = new Path(dirPath)
    val hdfs = org.apache.hadoop.fs.FileSystem.get(srcDir.toUri,sc.hadoopConfiguration)
    println(s"success ${hdfs.rename(srcDir,new Path("fake_hdfs/dontexists/file_for_rename_dst"))}")
  }
}
