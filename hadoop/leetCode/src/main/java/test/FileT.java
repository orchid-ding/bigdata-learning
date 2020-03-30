package test;

import com.sun.source.tree.SynchronizedTree;

import java.math.BigDecimal;
import java.util.*;

public class FileT {
    
    private static String name = "公共：\n" +
            "立白洗衣液：28.9\n" +
            "烧水壶： 55\n" +
            "一架：33.2\n" +
            "垃圾桶、刷子、垃圾袋：23.3\n" +
            "水果刀：9.9\n" +
            "纸巾： 28.65\n" +
            "牙刷：9.4\n" +
            "哈密瓜：28.9\n" +
            "赵：\n" +
            "被芯：74.3\n" +
            "枕头：47.4\n" +
            "我的：\n" +
            "背心： 113\n" +
            "毛巾：20.7\n" +
            "拖鞋：18.65\n" +
            "总共：";
    List<Float> floats = new ArrayList<>();
    static float numbers = 0f;

    public static void main(String[] args) {
        // 一个字符串，内容与此字符串相同，但一定取自具有唯一字符串的池。
        String a = "a";
        String b = "b";
        String ab = "ab";
        String ab0 = "a" + "b";
        String ab1 = a + b;
        String ab2 = new String("ab");
        String ab3 = new String("a") + new String("b");
        System.out.println(ab == ab0);
        System.out.println(ab == ab1.intern());
        System.out.println(ab == ab2.intern());
        System.out.println(ab == ab3.intern());
        System.out.println(ab2.intern() == ab3.intern());

        LinkedList linkedList = new LinkedList();
        linkedList.add(1);
        linkedList.get(2);
        List<String> list = Collections.synchronizedList(new ArrayList<>());

    }
    
}
