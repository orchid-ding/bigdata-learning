package output;

import java.sql.*;

public class MysqlUtils {

    private static Connection conn = null;

    public static Connection getConn(){
        if(conn == null){
            try {
                Class.forName("com.mysql.jdbc.Driver");
                conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/kfly?useSSL=false&serverTimezone=UTC","root","lancao");
            } catch (SQLException | ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
        return conn;
    }

    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        getConn();
        ResultSet resultSet = conn.createStatement().executeQuery("select * from kfly.stu");
        while (resultSet.next()){
            int id = resultSet.getInt("id");
            String name = resultSet.getString("name");
            System.out.println("id=>"+id+",name=>"+name);
        }
        conn.close();
//        insert();
    }


    public static void insert(int id,String name){
        try {
            getConn();
            PreparedStatement ps = conn.prepareStatement("insert into kfly.stu values(?,?)");
            ps.setInt(1,id);
            ps.setString(2,name);
            ps.execute();
            ps.close();
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }
}
