import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.junit.Before;
import org.junit.Test;
public class HiveJdbcCli {
    private Connection connection;
    private PreparedStatement ps;
    private ResultSet rs;
    @Before
    public void getConnection() {
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            connection = DriverManager.getConnection("jdbc:hive2://172.17.11.156:10000/zzq", "hive", "123456");
            System.out.println(connection);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    public void close() {
        try {
            if (rs != null) {
                rs.close();
            }
            if (ps != null) {
                ps.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    @Test
    public void createTable() {
        String sql = "create table goods2(id int,name string) row format delimited fields terminated by '\t' ";
        try {
            ps = connection.prepareStatement(sql);
            ps.execute(sql);
            //close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    @Test
    public void dropTable() {
        String sql = "drop table goods2";
        try {
            ps = connection.prepareStatement(sql);
            ps.execute();
            close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    //添加数据
    @Test
    public void insert() throws SQLException{
        String sql = "load data inpath '/goods.txt' into table goods";
        ps = connection.prepareStatement(sql);
        ps.execute();
        close();
    }
    //查询
    @Test
    public void find() throws SQLException {
        String sql = "select * from goods ";
        ps = connection.prepareStatement(sql);
        rs = ps.executeQuery();
        while (rs.next()) {
            System.out.println(rs.getObject(1) + "---" + rs.getObject(2));
        }
        close();
    }    
}
 