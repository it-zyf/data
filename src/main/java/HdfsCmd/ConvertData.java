package HdfsCmd;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.JDBCUtil;

import java.sql.Connection;
import java.sql.Statement;

/**
 * @Author twh
 * @Date 2020/4/6 15:18
 */
public class ConvertData {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private  String sql="insert into %s partition (day=%s) select * from %s where logTime>=%s";
    private  String createpartsql="alter table %s add partition (day_time='%s') location '/log/faith_server/%s/%s'";
    public ConvertData(String table,String tabletmp,String day,String starttime){
        this.sql = String.format(sql, table,day,tabletmp,starttime);
        System.out.println(this.sql);
    }
    public ConvertData(String table,String starttime){
        this.createpartsql = String.format(createpartsql, table,starttime,table,starttime);
        System.out.println(this.createpartsql);
    }
    public String ImplConvertData(){
       int state=0;
       try{
           Connection conn = JDBCUtil.getConn();
           Statement stmt = JDBCUtil.getStmt(conn);

           //执行sql语句
           state = stmt.executeUpdate(sql);//返回执行的结果集
           System.out.println("寫入ORC狀態——————————————————————"+state);
           JDBCUtil.closeFunc(conn,stmt);
       }catch (Exception e)
       {
           logger.error(e.getMessage());
       }
       return "狀態——————————————————————"+state;
    }
    public String ImplCreatePart(){
        int state=0;
        try{
            Connection conn = JDBCUtil.getConn();
            Statement stmt = JDBCUtil.getStmt(conn);

            //执行sql语句
            state = stmt.executeUpdate(createpartsql);//返回执行的结果集
            System.out.println("创建分区——————————————————————"+state);
            JDBCUtil.closeFunc(conn,stmt);
        }catch (Exception e)
        {
            //logger.error(e.getMessage());
        }
        return "狀態——————————————————————"+state;
    }
}
