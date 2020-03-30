package test;

import java.lang.reflect.Array;
import java.nio.channels.FileLock;
import java.util.ArrayList;
import java.util.List;

/**
 * @author dingchuangshi
 */
public class SonSet {

    public static List<List<Float>> getAll(Float[] values){
        List<Float> aList=new ArrayList<Float>();
        List<Float> bList=new ArrayList<Float>();
        for(int i=0;i<values.length;i++){
            aList.add(values[i]);
        }
        //方法1，递归法
        getSonSet2(values,values.length);
        return null;
    }

    /**
     * 递归法
     * @param i
     * @param aList
     * @param bList
     */
    public static void getSonSet1(int i,List<Float> aList,List<Float> bList){
        if(i>aList.size()-1){
            if(bList.size()<=0){
                System.out.print("@");
            }else {
                for(int m=1;m<bList.size();m++){
                    System.out.print(","+bList.get(m));
                }
            }
            System.out.println();
        }else {
            bList.add(aList.get(i));
            getSonSet1(i+1, aList, bList);
            int bLen=bList.size();
            bList.remove(bLen-1);
            getSonSet1(i+1, aList, bList);
        }
    }

    /**
     * 按位对应法
     * @param arr
     * @param length
     */
    private static void getSonSet2(Float[] arr, int length) {
        List<List<Float>> values = new ArrayList<>();
        int mark=0;
        int nEnd=1<<length;
        boolean bNullSet=false;
        for(mark=0;mark<nEnd;mark++){
            bNullSet=true;
            for(int i=0;i<length;i++){
                List<Float> value = new ArrayList<>();
                float va = 0f;
                //该位有元素输出
                if(((1<<i)&mark)!=0){
                    bNullSet=false;
                    values.add(value);
                    System.out.print(arr[i]+",");
                }
            }
            //空集合
            if(bNullSet){
                System.out.print("@");
            }
            System.out.println();
        }
    }
}