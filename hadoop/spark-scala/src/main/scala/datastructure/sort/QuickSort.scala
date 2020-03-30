package datastructure.sort

object QuickSort {

  def main(args: Array[String]): Unit = {

  }

  def quickSort(arr:List[Int]):List[Int] = {
    if (arr.length < 2) arr
    else
      quickSort(arr.filter(_ < arr.head)) ++
        arr.filter(_ == arr.head) ++
        quickSort(arr.filter(_ > arr.head))
  }
}
