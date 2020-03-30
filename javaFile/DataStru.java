public class DataStru{

    /**
     * 冒泡排序
     */
    public static void sort(int[] array){
        for(int i =0;i < array.length - 1; i++){
            for(int j = 0; j < array.length - i -1 ; j++){
                if(array[j] > array[j+1]){
                    int temp = array[j];
                    array[j] = array[j+1];
                    array[j+1] = temp;
                }
            }
        }
    }

    public static void main(String[] args){
        int[] data = { 9, -16, 21, 23, -30, -49, 21, 30, 30 };
        sort(data);
        for (int i = 0; i < data.length; i++) {
            System.out.println(data[i]);
        }
    }
}