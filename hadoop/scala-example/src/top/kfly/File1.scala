package top.kfly

import java.io.File


class File1 {

}
object File2RichFile{

  implicit def file2RichFile(file:File)=new RichFile(file)

}
class RichFile(val file:File){

  def read():String = {
    println(1)
    "1"
  }

}

object RichFile{
  def main(args: Array[String]): Unit = {
    val file:File = new File("aa.txt")
    import top.kfly.File2RichFile.file2RichFile
      println(file.read)

  }
}
