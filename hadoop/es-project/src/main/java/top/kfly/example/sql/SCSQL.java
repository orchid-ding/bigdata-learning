package top.kfly.example.sql;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.elasticsearch.xpack.sql.jdbc.EsDataSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class SCSQL {

    private static EsDataSource dataSource;

    @Before
    public void init() throws UnknownHostException, SQLException {
        dataSource  = new EsDataSource();
        String address = "jdbc:es://http://node01:9200" ;
        dataSource.setUrl(address);
        dataSource.setProperties(new Properties());

    }

    @Test
    public  void query() throws SQLException {
        Statement statement = dataSource.getConnection().createStatement();
        ResultSet resultSet = statement.executeQuery("select * from library");
        while(resultSet.next()){
            String string = resultSet.getString(0);
            String string1 = resultSet.getString(1);
            int anInt = resultSet.getInt(2);
            String string2 = resultSet.getString(4);
            System.out.println(string + "\t" +  string1 + "\t" +  anInt + "\t" + string2);
        }
        statement.close();

    }

    @After
    public void close(){
    }



}
