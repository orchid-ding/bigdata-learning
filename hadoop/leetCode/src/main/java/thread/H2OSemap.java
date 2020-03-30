package thread;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Semaphore;

class H2OSemap {

    private CyclicBarrier cyclicBarrier;
    private Semaphore semaphoreH;
    private Semaphore semaphoreO;

    public H2OSemap() {
        // 等待两个H分子进入
        this.semaphoreH = new Semaphore(2);
        // 等待一个O分子进入
        this.semaphoreO = new Semaphore(0);
    }

    public void hydrogen(Runnable releaseHydrogen) throws InterruptedException {
		semaphoreH.acquire();
        // releaseHydrogen.run() outputs "H". Do not change or remove this line.
        releaseHydrogen.run();
        if(semaphoreH.availablePermits() == 0){
            semaphoreO.release();
        }
    }

    public void oxygen(Runnable releaseOxygen) throws InterruptedException {
        semaphoreO.acquire();
        // releaseOxygen.run() outputs "O". Do not change or remove this line.
		releaseOxygen.run();
        semaphoreH.release(2);
    }


    public static void main(String[] args) {
        H2OSemap h2O = new H2OSemap();
        for (int i = 0; i < 100; i++) {
            Runnable runnable = new Runnable() {
                @Override
                public void run() {

                    try {
                        h2O.hydrogen(new Runnable() {
                            @Override
                            public void run() {
//                                System.out.print("H");
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
//                                    System.out.print("O");
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