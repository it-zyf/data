package utils;

/**
 * @Author twh
 * @Date 2020/4/4 17:01
 */
import java.sql.*;


public class JDBCUtil {
    static final String DriverName="org.apache.hive.jdbc.HiveDriver";
    static final String url="jdbc:hive2://cdh-slave1:10000/faith_server";
    static final String user="";
    static final String pass="";

    /**
     * 创建连接
     * @return
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    public static Connection getConn() throws ClassNotFoundException, SQLException {
        Class.forName(DriverName);
        Connection connection = DriverManager.getConnection(url,user,pass);
        return connection;
    }

    /**
     * 创建命令
     * @param connection
     * @return
     * @throws SQLException
     */
    public static Statement getStmt(Connection connection) throws SQLException {
        return connection.createStatement();
    }

    /**
     * 关闭连接
     * @param connection
     * @param statement
     * @throws SQLException
     */
    public static void closeFunc(Connection connection, Statement statement) throws SQLException {
        statement.close();
        connection.close();
    }


    public static void main(String[] args) throws SQLException, ClassNotFoundException {


    }
}