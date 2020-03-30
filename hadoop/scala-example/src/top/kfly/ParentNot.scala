package top.kfly

sealed class ParentNot {

  var name:String = _
  var age:Int = _
  def this(name:String,age:Int)={
    this()
    this.name = name
    this.age = age
}

}

object ParentNot{
  def apply(name: String, age: Int) : ParentNot = new ParentNot(name,age )


  def unapply(arg: ParentNot): Option[(String,Int)] = Some(arg.name,arg.age)

}

class Super
class Sub extends Super
class Parent[T]{

 Int
}


