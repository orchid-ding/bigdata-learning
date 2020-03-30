package datastructure.sort

object QuickSortScala {

  // 快速排序：它的基本思想是：通过一趟排序将要排序的数据分割成独立的两部分，其中一部分的所有数据都比另外一部分的所有数据都要小，
  // 然后再按此方法对这两部分数据分别进行快速排序，整个排序过程可以递归进行，以此达到整个数据变成有序序列。
  def quickSort(a:List[Int]):List[Int]={
    if (a.length < 2) a
    else quickSort(a.filter(_ < a.head)) ++
      a.filter(_ == a.head) ++
      quickSort(a.filter(_ > a.head))
  }

  def main(args: Array[String]): Unit = {
    println(quickSort(List(1,3,2,9,5,7)))
  }
}
