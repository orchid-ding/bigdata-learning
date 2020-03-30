package thread;

import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 哲学家就餐问题
 */
class DiningPhilosophers {

    /**
     * 资源： 一共有五个叉子，需要对叉子进行锁，同一个叉子有且只能被一个人持有
     */
    private ReentrantLock[] lock = {
            new ReentrantLock(),
            new ReentrantLock(),
            new ReentrantLock(),
            new ReentrantLock(),
            new ReentrantLock()
    };

    private ReentrantLock reentrantLock = new ReentrantLock();
    /**
     * 防止死锁： 同时只能有四个人拿到叉子（最坏情况）
     */
    private Semaphore semaphore = new Semaphore(4);

    public DiningPhilosophers() {
        
    }

    /**
     *
     * @param philosopher 人员编号
     * @param pickLeftFork 左边叉子
     * @param pickRightFork 右边叉子
     * @param eat 吃饭 执行
     * @param putLeftFork 放下左边叉子
     * @param putRightFork 放下右边叉子
     * @throws InterruptedException
     */
    public void wantsToEat(int philosopher,
                           Runnable pickLeftFork,
                           Runnable pickRightFork,
                           Runnable eat,
                           Runnable putLeftFork,
                           Runnable putRightFork) throws InterruptedException {
        reentrantLock.lock();
        //start  获取叉子编号
        // 左边叉子
        int leftFork = (philosopher + 1) % 5;
        // 右边叉子
        int rightFork = philosopher;

        semaphore.acquire(); // 可进入数 -1

        // 拿起叉子
        lock[leftFork].lock();
        lock[rightFork].lock();

        pickLeftFork.run();
        pickRightFork.run();

        // 吃
        eat.run();

        //放下叉子
        putLeftFork.run();
        putRightFork.run();

        lock[leftFork].unlock();
        lock[rightFork].unlock();

        semaphore.release(); // 可进入数 +1
        reentrantLock.unlock();

    }
}