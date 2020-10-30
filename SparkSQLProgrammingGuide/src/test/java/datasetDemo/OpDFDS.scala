package datasetDemo

import cases.{Employer, Person, Student, StudentScore}
import org.apache.commons.lang3.RandomStringUtils
import org.apache.spark.sql.{SparkSession, TypedColumn}
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

import scala.collection.mutable
import scala.util.Random

/**
 * @author dominiczhu
 * @date 2020/8/26 7:27 下午
 */
class OpDFDS {
  implicit val sparkConf = new SparkConf().setAppName("local").setMaster("local[5]")
  implicit val sc = new SparkContext(sparkConf)
  implicit val spark = SparkSession.builder().config(sparkConf).getOrCreate()

  import spark.implicits._

  @Test
  def createDsWithSeq(): Unit = {
    val df = sc.parallelize(Seq((4, 5, 6))).toDF("f1", "f2", "f3")
  }
  @Test
  def createDsWithTuple(): Unit = {
    val rdd = sc.parallelize(Seq((Person("person", 12L), Student("student", 12)))) //这种情况里表头是_1,_2，因为传入的是tuple
    rdd.toDS().show()
  }

  @Test
  def testSql():Unit={
    val df = sc.parallelize(Seq((4L, 5d, 6,"a","1"))).toDF()
    df.where($"_5"===1).show()
    df.where($"_5"equalTo (1)).show()
    df.where($"_5"equalTo (1)).show()
  }

  @Test
  def testRowType():Unit={
    val df = sc.parallelize(Seq((4L, 5d, 6,"a"))).toDF()
//    下面四种方法都会报错，因为row的类型只要不匹配，转换都会失败
//    df.map(row=>{
//      row.getAs[Double](0)
//    }).show()

//    df.map(row=>{
//      row.getAs[Double](2)
//    }).show()

//    df.map(row=>{
//      row.getAs[Long](2)
//    }).show()

//    df.map(row=>{
//      row.getLong(2)
//    }).show()
  }

  @Test
  def createWithCaseClass(): Unit = {
    val rdd = sc.parallelize(Seq(Student("person", 13), Student("student", 12)))

    val df = rdd.toDF()
    df.show()
    df.toDF("newName","newAge").show()

    val row = df.take(1)(0)
    println(row.getInt(0), row.getInt(1))
    //    println(row.getInt(0), row.getString(1))
    //    println(row.getDouble(0), row.getDouble(1))
  }
  @Test
  def createWithNestedCaseClass(): Unit = {
    val studentScoreDs = Seq(StudentScore(Student("mike", 19), 89), StudentScore(Student("tom", 20), 87)).toDS()

  }

  @Test
  def select():Unit={
    val rdd = sc.parallelize(Seq(Student("person", 13), Student("student", 12)))
    val df = rdd.toDF()
    val row1Name="stuName"
    df.select(col("stuName"),$"stuAge").show()
  }

  @Test
  def opDfWithNull():Unit={
    val df = sc.parallelize(Seq((4, 5, null))).toDF("f1", "f2", "f3")
    for (row <- df.take(3)) {
      println("==============")
      val f3Null = row.getAs[Any]("f3")
      println("f3Null", f3Null)
      val f3GetAsName = row.getAs[Int]("f3")
      println("f3GetAsName", f3GetAsName)// 也就是说如果直接取值赋值，那结果解释0
      println("what the fuck?", row.getAs[Int]("f3"))  //但如果直接打印，实际上返回的null

      val f3GetAsI = row.getAs[Int](2)
      println("f3GetAsI", f3GetAsI)
      println("what the fuck?", row.getAs[Int](2))
    }

    val anotherDf = df.map(row=>{
      row.getAs[Int]("f3")
    })
    anotherDf.show() // 结果就是0

    val anotherDf2=df.map(row=>{
      val f3GetAsName = row.getAs[Int]("f3")
      f3GetAsName
    })
    anotherDf2.show(1) // 结果也是0

    // 那如何才能顺利地获得null呢 下面这个方法是不可以的，因为这个方法返回的是Any类型的，但实际上dataset无法持有any类型的变量
//    df.map(row=>{
//      if (row.isNullAt(2))
//        null
//      else
//        row.getAs[Long](2)
//    }).show()

    // 第三个参数一定得是java lang的Integer，因为如果只写一个10，那么就是Int类型，其父类为AnyVal，但null是AnyRef的子类
    // 那么df2要持有的就是Any类型才行，但是dataset无法持有any类型的对象
    val df2 = sc.parallelize(Seq((4, 5, null),(4, 5,new java.lang.Integer(10)))).toDF("f1", "f2", "f3")
    // 为什么这样就可以了呢，因为这样这个生成的df持有的对象就是AnyRef了，null是anyref的子类，没毛病
    df2.map(row=>{
      if(row.isNullAt(2))
        null
      else
        row.getAs[java.lang.Integer](2)
    }).show()
    df2.map(row=>{
        row.getAs[java.lang.Integer](2)
    }).show()

    //下面也会报错，因为get返回的是Any类型
//    df2.map(row=>{
//      row.get(2)
//    }).show()
    // 这样就没问题了，get(2)返回的是Any类型，可以对其判定是否为null
    df2.map(row=>{
      val a=row.get(2)
      if(a==null)
        5
      else
        row.getInt(2)
    }).show()
  }

  @Test
  def unionDemo(): Unit = {
    val ds1 = sc.parallelize(Seq((1, 2, 3), (4, 5, 6))).toDS()
    val ds2 = sc.parallelize(Seq((1, 2, 3), (4, 5, 6))).toDS()
    ds1.union(ds2).show()
  }

  @Test
  def filterDemo() = {
    val ds = sc.parallelize(Seq(Student("s1", 12), Student("s2", 12), Student("s3", 12))).toDS()
    val res = ds.filter("stuName in ('s1','s2')").filter("stuName='s2'")
    res.show()
  }
  @Test
  def otherOperations() = {
    val df = sc.parallelize(Seq(Employer("s1", "xian", 1, 3000), Employer("s2", null, 1, 5000), Employer(null, "shagnhai", 0, 9000), Employer(null, null, 0, 1000))).toDF()
    df.na.fill("NA", Seq("name")).show()
    df.na.fill(Map("name" -> "emptyName", "city" -> "emptyCity")).show()
    df.na.drop(1, Seq("name", "city")).show() // name 与 city列至少有1个NonNull的row会留下

    df.na.replace(Seq("name", "city"), Map("s1" -> "s", "s2" -> "ss", "xian" -> "Xian")).show()
  }
  @Test
  def mapDemo() = {
    val df = sc.parallelize(Seq(Student("s1", 12), Student("s2", 18), Student("s3", 11), Student("s3", 15))).toDF()
    df.map(row => {
      row.getString(0)
    }).show()

    val maxValue = df.reduce((r1, r2) => {
      val (age1, age2) = (r1.getAs[Int]("stuAge"), r2.getAs[Int]("stuAge"))
      if (age1 > age2)
        r1
      else
        r2
    }).get(1)
    println(maxValue)

    df.rdd.map(r => {
      (r.getAs[String]("stuName"), r.getAs[Int]("stuAge"))
    }).reduceByKey((r1, r2) => {
      r1 + r2
    }).toDF().show()
  }

  @Test
  def mapWithEmptyDemo() = {
    val df = sc.parallelize(Seq(Student("s1", 12), Student("s2", 18), Student("s3", 11), Student("s3", 15))).toDF().filter($"stuAge">100)
//    不会报错，因为map里根本没有执行
    df.map(row => {
      1/0
    }).show()
  }

  @Test
  def lazyDemoWithLimit() = {
    // 这种情形，在local模式不会出现异常，但是在集群模式下，下面的几个persist之前的show的结果是有可能不同的，因为每次show，之前的这些操作都会重新跑一次，那由于limit的存在，不一定最后返回的是谁
    var studentSeq: mutable.Seq[Student] = mutable.Seq()
    for (age <- Array.range(20, 40)) {
      studentSeq = studentSeq :+ Student(RandomStringUtils.randomAlphabetic(10), Random.nextInt(20))
    }
    val df = sc.parallelize(studentSeq.toSeq).toDF()
    var shown = df.limit(2)
    shown.show()

    shown.show()
    shown.map(row => {
      Student(row.getAs[String]("stuName"), row.getAs[Int]("stuAge") + 100)
    })
    shown.show()

    shown.show()
    shown.show()
    shown.map(row => {
      Student(row.getAs[String]("stuName"), row.getAs[Int]("stuAge") + 100)
    })
    shown.show()

  }

  @Test
  def groupByKey():Unit={
    //groupByKey的功能就是自定义一个key，把相同key的row聚合起来进行操作
    val df = sc.parallelize(Seq(Student("s1", 11), Student("s2", 200), Student("s3", 120),Student("s4", 220),Student("s4", 1200))).toDF()
    val kvDf=df.groupByKey(row=>{
      val age=row.getAs[Int]("stuAge")
      if(age<10)
        "level1"
      else if(age<100)
        "level2"
      else if(age<1000)
        "level3"
      else if(age<10000)
        "level4"
      else
        "level5"
    })

    kvDf.count().show()

    kvDf.agg(mean("stuAge").as(newDoubleEncoder)).show()// 一定要用as来将一个普通的colmn变为typedColumn，具体的encoder的意义见api
    kvDf.mapGroups((s,iterRows)=>{
      var sum=0
      for(row<-iterRows){
        sum+=row.getInt(1)
      }
      (s,sum)
    }).show()
  }

  @Test
  def emptyDsGroupBy():Unit={
    val df = sc.parallelize(Seq(Student("s1", 11), Student("s2", 200), Student("s3", 120),Student("s4", 220),Student("s4", 1200))).toDF()
    df.filter(_=>false).agg(sum("stuAge")).show()
  }

}
