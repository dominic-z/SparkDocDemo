package gettingStarted

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Encoder, Encoders, Row, SparkSession}
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructField, StructType}

object AggregationsDemo {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("local").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    spark.udf.register("myAverage", MyAverage1)

    val df = spark.read.json("employees.json")
    df.createOrReplaceTempView("employees")
    df.show()

    val result1 = spark.sql("SELECT myAverage(salary) as average_salary1 FROM employees")
    result1.show()

    val ds = df.as[Employee]
    val averageSalary = MyAverage2.toColumn.name("average_salary2")
    val result2 = ds.select(averageSalary)
    result2.show()
  }

}

object MyAverage1 extends UserDefinedAggregateFunction {
  // Data types of input arguments of this aggregate function
  def inputSchema: StructType = StructType(StructField("inputColumn", LongType) :: Nil)

  // Data types of values in the aggregation buffer
  def bufferSchema: StructType = {
    StructType(StructField("sum", LongType) :: StructField("count", LongType) :: Nil)
  }

  // The data type of the returned value
  def dataType: DataType = DoubleType

  // Whether this function always returns the same output on the identical input
  def deterministic: Boolean = true

  // Initializes the given aggregation buffer. The buffer itself is a `Row` that in addition to
  // standard methods like retrieving a value at an index (e.g., get(), getBoolean()), provides
  // the opportunity to update its values. Note that arrays and maps inside the buffer are still
  // immutable.
  def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 0L
  }

  // Updates the given aggregation buffer `buffer` with new input data from `input`
  def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0)) {
      buffer(0) = buffer.getLong(0) + input.getLong(0)
      buffer(1) = buffer.getLong(1) + 1
    }
  }

  // Merges two aggregation buffers and stores the updated buffer values back to `buffer1`
  // 个人理解：比如说数据存储于两个node里，在这两个node里的数据聚合完毕后，获得了两个bufferSchema，然后按照什么方法把这两个bufferSchema和在一起，不过我不明白的是，为什么这个merge方法的第二个参数是Row类型的。
  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  // Calculates the final result
  def evaluate(buffer: Row): Double = buffer.getLong(0).toDouble / buffer.getLong(1)
}

case class Employee(name: String, salary: Long)

case class Average(var sum: Long, var count: Long)

object MyAverage2 extends Aggregator[Employee, Average, Double] {
  // A zero value for this aggregation. Should satisfy the property that any b + zero = b
  def zero: Average = Average(0L, 0L)

  // Combine two values to produce a new value. For performance, the function may modify `buffer`
  // and return it instead of constructing a new object
  def reduce(buffer: Average, employee: Employee): Average = {
    buffer.sum += employee.salary
    buffer.count += 1
    buffer
  }

  // Merge two intermediate values
  def merge(b1: Average, b2: Average): Average = {
    b1.sum += b2.sum
    b1.count += b2.count
    b1
  }

  // Transform the output of the reduction
  def finish(reduction: Average): Double = reduction.sum.toDouble / reduction.count

  // Specifies the Encoder for the intermediate value type Encoders里有很多内置的类，点进去看注释就能明白意思了
  def bufferEncoder: Encoder[Average] = Encoders.product

  // Specifies the Encoder for the final output value type
  def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}