package dataStructure.stack.recursion;

import org.junit.Test;

/**
 * 斐波那契数列
 * F(1)=1，F(2)=1, F(n)=F(n-1)+F(n-2)（n>=3，n∈N*）
 * @author dingchuangshi
 */
public class RecursionFibonacciTest {

    @Test
    public void fibonacciTest(){
        System.out.println(fibonacci(70));
    }


    @Test
    public void fibonacci2Test(){
        System.out.println(fibonacci(70,1,1));
    }

    @Test
    public void fibonacci3Test(){
        System.out.println(fibonacci3(70));
    }

    /**
     * 斐波那契数
     *递归函数
     */
    public int fibonacci(int n){
        if(n == 1 || n == 2){
            return 1;
        }
        return fibonacci(n-1) + fibonacci(n - 2);
    }

    /**
     * 尾递归
     * 将递归的结果存储下来，避免压栈
     */
    public int fibonacci(int n,int a,int b){
        if(n == 1) {
            return a;
        }
        return fibonacci(n-1,b,a + b);
    }

    /**
     * 循环递归法
     * @param n
     * @return
     */
    public int fibonacci3(int n){
        int result = 0;
        int pre = 1;
        int curr = 1;
        for (int i = 1; i <= n; i++) {
            if(i == 1){
                result = pre;
            }else if(i == 2){
                result = curr;
            }else {
                result = pre + curr;
                pre = curr;
                curr = result;
            }
        }
        return result;
    }
}
