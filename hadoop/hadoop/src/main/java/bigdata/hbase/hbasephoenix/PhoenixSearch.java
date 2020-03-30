package bigdata.hbase.hbasephoenix;

import java.sql.*;

/**
 * @author dingchuangshi
 */
public class PhoenixSearch {

    public static void main(String[] args) throws SQLException {
        //定义phoenix的连接url地址
        String url="jdbc:phoenix:node01:2181";
        Connection connection = DriverManager.getConnection(url);
        //构建Statement对象
        Statement statement = connection.createStatement();
        //定义查询的sql语句，注意大小写
        String sql="select * from US_POPULATION";
        //执行sql语句
        try {
            ResultSet rs = statement.executeQuery(sql);
            while(rs.next()){
                System.out.println("state:"+rs.getString("state"));
                System.out.println("city:"+rs.getString("city"));
                System.out.println("population:"+rs.getInt("population"));
                System.out.println("-------------------------");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            if(connection!=null){
                connection.close();
            }
        }

    }
}