package datastructure.sort;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author dingchuangshi
 */
public class QuickSortJava {

    public static void main(String[] args) {
        int[] arr = {1,2,3,4,12,3,25,45,4,54,6,1,2};
        quickSort(arr,0,arr.length-1);
        for (int i = 0; i < arr.length; i++ ){
            System.out.print(arr[i] + ",");
        }
    }

    /**
     * @param arr 要排序的数组
     * @param left 左边索引
     * @param right 右边索引
     */
    public static void quickSort(int[] arr,int left,int right){
        if(left > right){
            return;
        }
        int value = arr[left];
        int i = left;
        int j = right;
        while(i!= j){
            // 1. 右边先走,寻找比value小的数
            while(arr[j] >= value && i < j){
                j--;
            }
            // 2. 左边寻找比value大的数
            while (arr[i] <= value && i < j){
                i++;
            }
            // 3. 交换数据
            if(i<j){
                int temp = arr[i];
                arr[i] = arr[j];
                arr[j] = temp;
            }
        }
        // 4. 这时 i==j ,第一位和i互换位置
        arr[left] = arr[i];
        arr[i] = value;
        // 5. 左边数据
        quickSort(arr,left,i-1);
        // 6. 右边数据
        quickSort(arr,i+1,right);
    }

    public void quickSort2(List<Integer> list){
        if(list.size() < 2){
            return;
        }else {
            quickSort2(list.stream().filter(x->x<list.get(0)).collect(Collectors.toList()));
        }
    }


}
