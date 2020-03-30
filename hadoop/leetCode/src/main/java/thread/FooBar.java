package thread;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Semaphore;

/**
 *
 * @author dingchuangshi
 */
public class FooBar {

    private int n;

    private Semaphore semaphore;
    private Semaphore semaphoreB;

    public FooBar(int n) {
        this.semaphore = new Semaphore(1);
        this.semaphoreB = new Semaphore(0);
        // 同时阻塞两个线程
        this.n = n;
    }
    public void foo(Runnable printFoo) throws InterruptedException {
        for (int i = 0; i < n; i++) {
            semaphore.acquire();
            printFoo.run();
            semaphoreB.release();
        }
    }

    public void bar(Runnable printBar) throws InterruptedException {
        for (int i = 0; i < n; i++) {
            semaphoreB.acquire();
            printBar.run();
            semaphore.release();
        }
    }
}