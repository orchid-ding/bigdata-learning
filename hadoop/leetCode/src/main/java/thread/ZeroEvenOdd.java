package thread;

import java.util.concurrent.Semaphore;
import java.util.function.IntConsumer;

/**
 * @author dingchuangshi
 */
public class ZeroEvenOdd {

    private int n;

    private Semaphore a,b,c;

    public ZeroEvenOdd(int n) {
        this.a = new Semaphore(1);
        this.b = new Semaphore(0);
        this.c = new Semaphore(0);
        this.n = n;
    }

    /**
     *     printNumber.accept(x) outputs "x", where x is an integer.
      */
    public void zero(IntConsumer printNumber) throws InterruptedException {
        for (int i = 0; i < n ; i++) {
            a.acquire();
            printNumber.accept(0);
            if((i&1) == 0){
                c.release();
            }else {
                b.release();
            }
        }
    }

    public void even(IntConsumer printNumber) throws InterruptedException {
        for (int i = 2; i < n; i+=2) {
            b.acquire();
            printNumber.accept(i);
            a.release();
        }
    }

    public void odd(IntConsumer printNumber) throws InterruptedException {
        for (int i = 1; i < n; i+=2) {
            c.acquire();
            printNumber.accept(i);
            a.release();
        }
    }

    public static void main(String[] args) {
        ZeroEvenOdd zeroEvenOdd = new ZeroEvenOdd(10);
        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                try {
                    zeroEvenOdd.zero((value -> System.out.println(value)));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };
        Runnable runnable1 = new Runnable() {
            @Override
            public void run() {
                try {
                    zeroEvenOdd.even((value -> System.out.println(value)));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };
        Runnable runnable2 = new Runnable() {
            @Override
            public void run() {
                try {
                    zeroEvenOdd.odd((value -> System.out.println(value)));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };

        new Thread(runnable).start();
        new Thread(runnable1).start();
        new Thread(runnable2).start();
    }

}