package thread;

import sun.misc.ThreadGroupUtils;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Semaphore;

class H2O {

    private CyclicBarrier cyclicBarrier;
    private Semaphore semaphoreH;
    private Semaphore semaphoreO;

    public H2O() {
        // 当生成一个水分子的线程足够后再去执行
        this.cyclicBarrier = new CyclicBarrier(3);
        // 等待两个H分子进入
        this.semaphoreH = new Semaphore(2);
        // 等待一个O分子进入
        this.semaphoreO = new Semaphore(1);
    }

    public void hydrogen(Runnable releaseHydrogen) throws InterruptedException {
		semaphoreH.acquire();
		try{
		    cyclicBarrier.await();
        }catch (Exception e){
		    e.printStackTrace();
        }
        // releaseHydrogen.run() outputs "H". Do not change or remove this line.
        releaseHydrogen.run();
		semaphoreH.release();
    }

    public void oxygen(Runnable releaseOxygen) throws InterruptedException {
        semaphoreO.acquire();
        try{
            cyclicBarrier.await();
        }catch (Exception e){
            e.printStackTrace();
        }
        // releaseOxygen.run() outputs "O". Do not change or remove this line.
		releaseOxygen.run();
        semaphoreO.release();
    }


    public static void main(String[] args) {
        H2O h2O = new H2O();
        for (int i = 0; i < 3; i++) {
            Runnable runnable = new Runnable() {
                @Override
                public void run() {

                    try {
                        h2O.hydrogen(new Runnable() {
                            @Override
                            public void run() {
                                System.out.print("H");
                            }
                        });
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            };
            Runnable runnable2= new Runnable() {
                @Override
                public void run() {
                        try {
                            h2O.oxygen(new Runnable() {
                                @Override
                                public void run() {
                                    System.out.print("O");
                                }
                            });
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                }
            };
            new Thread(runnable).start();
            new Thread(runnable2).start();
        }

    }
}