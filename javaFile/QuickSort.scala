object quickSort{
    def quickSort(data:List[Int]):List[Int] = {
        if (data.length < 2){
            data
        } else{
            quickSort(data.filter(_ < data.head)) 
            ++ data.filter(_ == data.head) 
            ++ quickSort(data.filter(_ > data.head))
        }
        
    }
    
def main(args: Array[String]): Unit = {
    println(quickSort(List(1,3,2,9,5,7)))
  }

}