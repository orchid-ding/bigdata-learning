import java.util.concurrent.TimeUnit;

public class CpuProcess {

//    public static void main(String[] args) throws InterruptedException {
//        for(;;){
//            for (int i = 0; i < 9600000; i++) {
//                ;
//            }
//            TimeUnit.MILLISECONDS.sleep(10);
//        }
//    }


    public static void main(String[] args) {
        Fu fu = new Zi();
        System.out.println(fu.getNum());
    }
}

class Fu{
    public int num = 100;
    public String str = "str_100";

    public int getNum() {
        return num;
    }
}

class Zi extends Fu{
    public int num = 1000;
    public String str = "str_200";
}