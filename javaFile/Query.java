class Query{

    private static int[] array = {1,8, 10, 89, 89, 1000, 1000,1234};

    private static int result = -1;
    public static void main(String[] args){
        query(89,0,array.length -1);
        System.out.println(result);
    }
    public static void query(int index,int left,int right){
        int mid = (left + right) / 2;
        if(left >= right){
            return;
        }

        if(index > array[mid]){
            query(index,mid,right);
        }else if(index < array[mid]){
            query(index,left,mid);
        }else{
            System.out.println(mid);
            for (int i = mid +1; i < array.length; i++) {
                if(array[mid] = array[i]){
                    System.out.println(i);
                }else{
                    break;
                }
            }
            for (int i = mid -1; i > 0; i--) {
                if(array[mid] = array[i]){
                    System.out.println(i);
                }else{
                    break;
                }
            }
        }
    }
}