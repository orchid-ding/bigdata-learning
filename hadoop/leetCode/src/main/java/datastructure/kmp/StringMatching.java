package datastructure.kmp;

import com.drew.metadata.heif.boxes.PrimaryItemBox;
import com.sun.tools.internal.xjc.reader.xmlschema.bindinfo.BIConversion;
import jdk.internal.org.objectweb.asm.tree.FieldInsnNode;

import java.util.stream.IntStream;

public class StringMatching {

    private static String s = "BBC ABCDAB ABCDABCDABDE";
    private static String p = "ABCDABD";

    public static void main(String[] args) {
        matching(s,p);
    }

    public static void matching(String s,String p){
        int p_size = p.length();
        int s_size = s.length();

        String[] p_s = p.split("");
        String[] s_s = s.split("");

        int i=0,j=0;
        while (i<s_size && j<p_size){
            if(s_s[i].equals(p_s[j])){
                i++;
                j++;
            }else {
                i = i-j+1;
                j = 0;
            }
        }
        if(j == p_size){
            System.out.println("index=>"+(i-j));
            System.out.println(s);
            for (int k = 0; k < (i-j); k++) {
                System.out.printf(" ");
            }
            System.out.println(p);
        }else {
            System.out.println("not found");
        }
    }

}
