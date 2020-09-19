import javax.ws.rs.GET
import org.junit.Test

import scala.collection.mutable

/**
 * @author dominiczhu
 * @date 2020/8/28 3:02 下午
 */
class Demo {
  @Test
  def show(): Unit ={
    val ds=20200812
    println("p_"+20200812)
  }

  @Test
  def atDemo(): Unit = {
    val o: Option[Int] = Some(2)
    o match {
      case Some(x) =>
        println(x)
      case None => println("test")
    }

    o match {
      case x@Some(_) =>
        println(x)
      case None => println("test")
    }
  }

  @Test
  def collectionConcatDemo() = {
    var xs = mutable.Set[Int]()
    var xs2 = mutable.Set[Int]()
    xs += 1
    println(xs2)
  }

  @Test
  def applyDemo() = {
    class CanApply {
      def apply(a: String): String = {
        "123"
      }
    }
    val canApply=new CanApply
    println(canApply("13"))
  }


}
