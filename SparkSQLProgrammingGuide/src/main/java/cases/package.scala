/**
 * @author dominiczhu
 * @date 2020/8/27 10:03 上午
 */
package object cases {
  case class Student(stuName:String,stuAge:Int)
  case class Person(name:String,age:Long)

  case class StudentScore(student: Student,score:Int)
}
