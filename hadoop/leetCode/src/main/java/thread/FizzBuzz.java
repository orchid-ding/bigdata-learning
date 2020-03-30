package thread;
import sun.awt.Mutex;

import java.util.concurrent.ThreadFactory;
import java.util.function.IntConsumer;

class FizzBuzz {
    private int n;
    private volatile  int index = 1;
    private Object lock = new Object();

    public FizzBuzz(int n) {
        this.n = n;
    }

    public void fizz(Runnable printFizz) throws InterruptedException {
        while (index <= n){
            synchronized (lock){
                if(index % 3 == 0 && index % 5 != 0) {
                    printFizz.run();
                    index ++;
                    lock.notifyAll();
                }
            }
        }
    }
    public void buzz(Runnable printBuzz) throws InterruptedException {
        while (index <= n){
            synchronized (lock){
                if(index % 3 != 0 && index % 5 == 0) {
                    printBuzz.run();
                    index ++;
                    lock.notifyAll();
                }
            }
        }
    }

    public void fizzbuzz(Runnable printFizzBuzz) throws InterruptedException {
        while (index <= n){
            synchronized (lock){
                if(index % 3 == 0 && index % 5 == 0) {
                    printFizzBuzz.run();
                    index ++;
                    lock.notifyAll();
                }
            }
        }
    }

    public void number(IntConsumer printNumber) throws InterruptedException {
        while (index <= n){
            synchronized (lock){
                if(index % 3 != 0 && index % 5 != 0) {
                    printNumber.accept(index);
                    index ++;
                    lock.notifyAll();
                }
            }
        }
    }
    // 主线程执行
    public static void main(String[] args) throws InterruptedException {
      ThreadFactory threadFactory = new ThreadFactory() {
          @Override
          public Thread newThread(Runnable r) {
              return new Thread(r);
          }
      };

      FizzBuzz fizzBuzz = new FizzBuzz(16);
        threadFactory.newThread(()-> {
            try {
                fizzBuzz.fizz(()-> System.out.printf("fizz,"));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }).start();
        threadFactory.newThread(()-> {
            try {
                fizzBuzz.buzz(()-> System.out.printf("buzz,"));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }).start();
        threadFactory.newThread(()-> {
            try {
                fizzBuzz.fizzbuzz(()-> System.out.printf("fizzbuzz,"));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }).start();
        threadFactory.newThread(()-> {
            try {
                fizzBuzz.number(value -> System.out.printf(value + ","));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }).start();
    }

}