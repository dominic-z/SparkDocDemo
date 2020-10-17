import ml.dmlc.xgboost4j.scala.spark.XGBoostClassifier
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.junit.Test

/**
 * @author dominiczhu
 * @date 2020/10/16 5:29 下午
 */
class ClassificationDemo {
  val sparkConf = new SparkConf().setAppName("local").setMaster("local[5]")
  val sc = new SparkContext(sparkConf)
  val spark = SparkSession.builder().config(sparkConf).getOrCreate()
  import spark.implicits._

  @Test
  def testSimpleTrain():Unit={
    val schema = new StructType(Array(
      StructField("sepal length", DoubleType, true),
      StructField("sepal width", DoubleType, true),
      StructField("petal length", DoubleType, true),
      StructField("petal width", DoubleType, true),
      StructField("class", StringType, true)))
    val rawInput = spark.read.schema(schema).csv("data/iris.csv")

    val stringIndexer = new StringIndexer().
      setInputCol("class").
      setOutputCol("classIndex").
      fit(rawInput)
    val labelTransformed = stringIndexer.transform(rawInput).drop("class")

    labelTransformed.show(10)

    val vectorAssembler = new VectorAssembler().
      setInputCols(Array("sepal length", "sepal width", "petal length", "petal width")).
      setOutputCol("features")
    val xgbInput = vectorAssembler.transform(labelTransformed).select("features", "classIndex")
//    说明xgboost的输入feature也得是org.apache.spark.ml.linalg.Vector
    print(xgbInput.schema)

    xgbInput.show(10)
    val xgbParam = Map("eta" -> 0.1f,
      "max_depth" -> 2,
      "objective" -> "multi:softprob",
      "num_class" -> 3,
      "num_round" -> 100,
      "num_workers" -> 2)
    val xgbClassifier = new XGBoostClassifier(xgbParam).
      setFeaturesCol("features").
      setLabelCol("classIndex")

    val xgbClassificationModel = xgbClassifier.fit(xgbInput)

  }

}
