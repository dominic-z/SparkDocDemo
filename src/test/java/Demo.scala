import org.junit.Test

/**
 * @author dominiczhu
 * @date 2020/8/28 3:02 下午
 */
class Demo {

  @Test
  def demo(): Unit = {
    println("%s".format(19))
  }
  def func():Boolean={
    for(i<-Array(1,2,3,4)){
      if(i>3)
        return true
    }
    false
  }

}
