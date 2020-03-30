public class JvmParam {

    public static void main(String[] args) {

        byte[] bytes = new byte[1024 * 1024 * 10];
        System.out.println("Xmx=" + Runtime.getRuntime().maxMemory() / 1024/1024 + "M");

        System.out.println("free=" + Runtime.getRuntime().freeMemory()/ 1024/1024 + "M");

        System.out.println("total=" + Runtime.getRuntime().totalMemory()/ 1024/1024 + "M");
    }
}
