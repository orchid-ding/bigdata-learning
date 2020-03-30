package datastructure.tanxin

abstract class Color

case object Red extends Color
case object Black extends Color

class RedBlackTree[T <% Ordered[T]] {
    abstract class Node
    case class Leaf() extends Node
    case class RBNode(color:Color, value: T, left:Node, right:Node) extends Node

    var root: Node  = Leaf()

    def search(value: T) = this.search_recursive(this.root, value)

    def search_recursive(node: Node, value: T): Boolean = node match {
            case Leaf() => false
            case RBNode(_, v, left, right) => 
                v == value || 
                (value < v && this.search_recursive(left, value)) ||
                (value>v && this.search_recursive(right, value))
    }

    def insert(value: T): Unit = {
        this.root = this.insert_recursive(this.root, value)
        this.root = this.root match {
            case RBNode(color, value, left, right) => RBNode(Black, value, left, right)
        }
    }

    def insert_recursive(node: Node, newval: T): Node = node match {
        case Leaf() => RBNode(Red, newval, Leaf(), Leaf())
        case RBNode(color, v, left, right) => 
            if (newval < v) balance(RBNode(color, v, insert_recursive(left, newval), right))
            else if (newval > v) balance(RBNode(color, v, left, insert_recursive(right, newval)))
            else node
    }

    def balance(node: Node): Node = node match {
        case RBNode(Black, z, RBNode(Red, y, RBNode(Red, x, a, b), c), d) => 
            RBNode (Red, y, RBNode(Black, x, a, b), RBNode(Black, z, c, d))

        case RBNode(Black, z, RBNode(Red, x, a, RBNode(Red, y, b, c)), d) =>
            RBNode (Red, y, RBNode(Black, x, a, b), RBNode(Black, z, c, d))

        case RBNode(Black, x, a, RBNode (Red, z, RBNode (Red, y, b, c), d)) =>
            RBNode (Red, y, RBNode(Black, x, a, b), RBNode(Black, z, c, d))

        case RBNode(Black, x, a, RBNode (Red, y, b, RBNode (Red, z, c, d))) =>
            RBNode (Red, y, RBNode(Black, x, a, b), RBNode(Black, z, c, d))
        case n: Node => n
    }

    def blackHeight(): Int = {
        var tmp = this.root
        var h = 0
        while(tmp.isInstanceOf[RBNode]) {
            tmp match {
                case RBNode(color, _, left, _) => {
                    tmp = left
                    h += 1
                }
            }
        }
        return h
    }
}