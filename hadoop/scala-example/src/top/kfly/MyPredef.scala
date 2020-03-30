package top.kfly

import java.io.File
import scala.io.Source

//todo:隐式转换案例一:让File类具备RichFile类中的read方法

object MyPredef{
  //定义一个隐式转换的方法，实现把File转换成RichFile
  implicit  def file2RichFile(file:File)=new RichFile(file)

}
