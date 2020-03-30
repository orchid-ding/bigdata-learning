package thread;


import java.util.concurrent.CountDownLatch;

/**
 * @author dingchuangshi
 */
public class Foo {

    private CountDownLatch countA,countB;

    public Foo(){
        countA = new CountDownLatch(1);
        countB = new CountDownLatch(1);
    }

    public void first(Runnable printFirst) throws InterruptedException {
        printFirst.run();
        countA.countDown();
    }

    public void second(Runnable printSecond) throws InterruptedException {
        countA.await();
        printSecond.run();
        countB.countDown();
    }

    public void third(Runnable printThird) throws InterruptedException {
        countB.await();
        printThird.run();
    }

    public static void main(String[] args) throws InterruptedException {
        Foo foo = new Foo();

        foo.third(() -> System.out.println("3"));
        foo.first(() -> System.out.println("1"));
        foo.second(() -> System.out.println(2));
    }
}