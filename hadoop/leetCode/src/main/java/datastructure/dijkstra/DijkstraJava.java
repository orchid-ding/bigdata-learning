package datastructure.dijkstra;

/**
 * @author dingchuangshi
 * 最短路径
 */
public class DijkstraJava {

    public static final int MAX = Integer.MAX_VALUE;

    public static void main(String[] args) {
        int[][] weigth = {
                {0,40,70,MAX,MAX},
                {50,0,15,30,MAX},
                {70,15,0,MAX,40},
                {MAX,30,MAX,0,20},
                {MAX,MAX,40,20,0}};

        Dijsktra(weigth,0);
    }

    public static  int[] Dijsktra(int [][] weight, int start){
        int length = weight.length;//获取顶点个数
        int[] shortPath = new int[length];//最短路径数组
        shortPath[0] = 0;//
        String[] path = new String[length];//记录起始点到各定点的最短路径
        for(int i = 0 ; i < length ; i++){
            path[i] = start + "->" + i;
        }
        int[] visited = new int[length];//记录当前顶点的最短路径是否已经求出，1表示已求出
        for(int i = 0 ; i < length ; i++){
            visited[i] = 0;
        }
        visited[0] = 1;//起始点的最短路径已经求出
        for(int m = 1 ; m < length ; m ++){
            int k = -1;
            int dmin = Integer.MAX_VALUE;
            //选择一个离起始点最近的未标记顶点，且到起始点的最短路径为dmin
            for(int n = 0 ; n < length ; n++){
                if(visited[n] == 0 && weight[start][n] < dmin){
                    dmin = weight[start][n];
                    k = n;
                }
            }
            shortPath[k] = dmin;

            visited[k] = 1;

            //以k为中间点，更新起始点到其他未标记各点的距离
            for(int j = 0 ; j < length ; j++){

                if(visited[j] == 0 && weight[k][j] != Integer.MAX_VALUE && weight[start][k] + weight[k][j] < weight[start][j]){

                    weight[start][j] = weight[start][k] + weight[k][j];

                    path[j] = path[k] + "->" + j;
                }
            }
        }

        for(int i = 1 ; i < length ; i ++){

            System.out.println("起始点到" + i + "的最短路径为:" + path[i] + "距离为：" + shortPath[i]);
        }
        return shortPath;
    }
}
