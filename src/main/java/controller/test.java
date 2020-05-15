package controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.ResourceUtils;
import org.springframework.web.bind.annotation.RestController;
import utils.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.lang.reflect.Field;
import java.sql.*;

@Component
@RestController
public class test {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private static final File file = new File("D:\\10203_serverRankListLog_2020042923_6356.json");
@Test
    public void tt() throws Exception {

    System.out.println(GetTime.getYearByday(-2));
    System.out.println(GetTime.getYesTeday(-1));
    }
    @Test
    public  void executeShell() throws SQLException, ClassNotFoundException {
        Connection conn = JDBCUtil.getConn();
        Statement stmt = JDBCUtil.getStmt(conn);

        //执行sql语句
        String sql1="alter table server_account_create add partition (day_time='20200429') location '/log/faith_server/server_account_create/20200429'";
         int state = stmt.executeUpdate(sql1);//返回执行的结果集
        System.out.println(state);

        JDBCUtil.getConn();
        System.out.println("sql");
    }

}
