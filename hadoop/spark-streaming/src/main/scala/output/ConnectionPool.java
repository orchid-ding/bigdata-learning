package output;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author dingchuangshi
 */
public class ConnectionPool {

    private static ComboPooledDataSource cpds = new ComboPooledDataSource();

    static{
        try {
            cpds.setDriverClass( "com.mysql.jdbc.Driver" );
        } catch (PropertyVetoException e) {
            e.printStackTrace();
        }
        cpds.setJdbcUrl( "jdbc:mysql://localhost:3306/kfly?useSSL=false&serverTimezone=UTC" );
        cpds.setUser("root");
        cpds.setPassword("lancao");

        cpds.setMinPoolSize(2);
        cpds.setInitialPoolSize(10);
        cpds.setMaxStatements(100);
    }

    public static Connection getConn(){
        Connection connection = null;
        try {
            connection = cpds.getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return connection;
    }

    public static void returnConn(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

}
