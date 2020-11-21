import org.junit.Test

/**
 * @author dominiczhu
 * @date 2020/8/28 3:02 下午
 */
class Demo {

  @Test
  def demo(): Unit = {
    var (a,b)=func()
    a=false
    println(a)
    println(b)
  }
  def func():(Boolean,Int)={
    (true,1)
  }

}
