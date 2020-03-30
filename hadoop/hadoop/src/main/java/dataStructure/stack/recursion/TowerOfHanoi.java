package dataStructure.stack.recursion;

import java.util.Stack;

/**
 * 汉诺塔问题
 * 游戏的目标：
 * 1。 把A杆上的金盘全部移到C杆上，并仍保持原有顺序叠好。
 * 2。操作规则：每次只能移动一个盘子，并且在移动过程中三根杆上都始终保持大盘在下，小盘在上，
 * 3。 操作过程中盘子可以置于A、B、C任一杆上。
 * @author dingchuangshi
 */
public class TowerOfHanoi {


    public void move(Stack source,Stack medium,Stack target,int n){
        print(source,medium,target);
         if(n == 1){
            target.push(source.pop());
         }else {
             move(source,target,medium,n-1);
             target.push(source.pop());
             move(medium,source,target,n-1);
         }
    }

    private void print(Stack source,Stack medium,Stack target){
        System.out.println(source.toString());
        System.out.println(target.toString());
        System.out.println(medium.toString());
        System.out.println("--------------------------------");
    }


    public static void main(String[] args) {
        // 首先有三个栈，用于模仿柱子
        Stack stackA = new Stack<Integer>();
        Stack stackB = new Stack<Integer>();
        Stack stackC = new Stack<Integer>();
        int n = 64;
        for (int i = n; i >=1 ; i--) {
            stackA.push(i);
        }

        TowerOfHanoi towerOfHanoi = new TowerOfHanoi();
        towerOfHanoi.move(stackA,stackB,stackC,n);
        towerOfHanoi.print(stackA ,stackB,stackC);

    }

}
