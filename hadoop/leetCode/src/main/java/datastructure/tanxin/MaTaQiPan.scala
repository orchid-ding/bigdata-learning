package datastructure.tanxin

import sun.jvm.hotspot.utilities.BitMap

import scala.swing.model.Matrix

/**
 * 马踏棋盘 8*8，所有可能
 */
object MaTaQiPan{

  /**
   * 当前位置的所有方向
   */
  val dn = List(
        (-2,-1),(-2,1), (-1,-2),(-1,2),
        (1,2),(1,-2), (2,1),(2,-1))

  case class stack(abscissa:Int // 横坐标
                   ,ordinate:Int // 纵坐标
                   ,val direction:Int // 方向
                  )
  /**
   *  初始化一个8*8的数据棋盘
   */
  val chessboard:Array[Array[Boolean]] = Array.ofDim(8,8)

  def main(args: Array[String]): Unit = {
  }
}

