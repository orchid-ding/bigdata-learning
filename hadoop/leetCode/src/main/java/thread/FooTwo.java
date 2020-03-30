package thread;


import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;

/**
 * @author dingchuangshi
 */
public class FooTwo {

    private Semaphore spA,spB;

    public FooTwo(){
        spA = new Semaphore(0);
        spB = new Semaphore(0);
    }

    public void first(Runnable printFirst) throws InterruptedException {
        printFirst.run();
        spA.release();
    }

    public void second(Runnable printSecond) throws InterruptedException {
        spA.acquire();
        printSecond.run();
        spB.release();
    }

    public void third(Runnable printThird) throws InterruptedException {
        spB.acquire();
        printThird.run();
    }

    public static void main(String[] args) throws InterruptedException {
        FooTwo foo = new FooTwo();

        foo.third(new Runnable() {
            public void run() {
                System.out.println("3");
            }
        });
        foo.first(new Runnable() {
            public void run() {
                System.out.println("1");
            }
        });
        foo.second(new Runnable() {
            public void run() {
                System.out.println(2);
            }
        });
    }
}