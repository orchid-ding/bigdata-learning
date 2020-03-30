import org.apache.oro.text.MatchActionProcessor
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.HashMap

object Text {
  def mergedSort[T](less: (T, T) => Boolean)(list: List[T]): List[T] = {

    def merged(xList: List[T], yList: List[T]): List[T] = {
      (xList, yList) match {
        case (Nil, _) => yList
        case (_, Nil) => xList
        case (x :: xTail, y :: yTail) => {
          if (less(x, y)) x :: merged(xTail, yList)
          else
            y :: merged(xList, yTail)
        }
      }
    }

    val n = list.length / 2
    if (n == 0) list
    else {
      val (x, y) = list.splitAt(n)
      merged(mergedSort(less)(x), mergedSort(less)(y))
    }
  }
//
//  def main(args: Array[String]) {
//    Text.getClass.getName
//    val list = List(3, 12, 43, 23, 7, 1, 2, 0)
//    println(mergedSort1(list))
////   list.splitAt(4) match {
////     case (x :: xTail, y :: yTail) => {
////       println(x)
////       println(xTail)
////       println(y)
////       println(yTail)
//     }


  def main(args:Array[String]){
    var conf = new SparkConf
    conf.setMaster("local[2]")
      conf.setAppName(Text.getClass.getName)

    val spark = new SparkContext(conf)
//    spark.textFile("input")
//      .flatMap(_.split(","))
//        .map((_,1))
//        .reduceByKey(_ + _)
//        .saveAsTextFile("/output")

    val value = spark.parallelize(List(('a', 1), ('a', 3), ('b', 3), ('b', 5), ('c', 4)))
    value.groupBy(_._1).map(x=>{
      (x._1,x._2.toList.map(_._2).sum / x._2.size)
    }).foreach(println)
  }

  def mergedSort1(data:List[Int]):List[Int]={

    def megred1(left:List[Int],right:List[Int]):List[Int]= {
      (left,right) match {
        case (_,Nil) => left
        case (Nil,_) => right
        case (x::xTail,y::yTail) => {
          if(x.asInstanceOf[Int] < y.asInstanceOf[Int]){
            x::megred1(xTail.asInstanceOf[List[Int]],right)
          } else{
            y::megred1(left,yTail.asInstanceOf[List[Int]])
          }
        }
      }
    }

    val n = data.length / 2
    if( n == 0){
      data
    }else{
      val (x,y) = data.splitAt(n)
      megred1(mergedSort1(x),mergedSort1(y))
    }

  }

}
