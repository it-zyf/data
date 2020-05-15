package controller;


import HdfsCmd.ConvertShell;
import Rsync.Sync;
import hivePutFile.PutFile;
import hivePutFile.PutImpala;
import lombok.extern.slf4j.Slf4j;
import model.bean.ServerCrossServerBossKill;
import model.bean.ServerRankListLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import queue.PrintTask;
import queue.TaskQueue;
import utils.GetDate;
import utils.GetFileContextUtil;
import utils.GetTime;
import utils.R;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;


@Component
@RestController
@Slf4j
public class CheckController {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    TaskQueue taskQueue = new TaskQueue(1);
    @RequestMapping("/pullhdfs")
    public  String pullhdfs(int day) {
        List<String> list  = null;
        try {
            list = GetFileContextUtil.getFileContext("/data/config/serverip.txt");
            String starttime= GetTime.gettime();
            for(String s:list) {
                PutFile.TransformationPutByday(s,day);
            }
            System.out.println("開始時間"+starttime);
            System.out.println("結束時間"+GetTime.gettime());
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
        return "123";
        }
    @RequestMapping("/rsyncfile")
    public  String rsyncfile() {
//
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//设置日期格式
        List<String> list  = null;
        try {
            list = GetFileContextUtil.getFileContext("/data/config/serverip.txt");

        logger.info("数据源一共:"+list.size());
        logger.info("开始同步时间是"+df.format(new Date()));
        Date startt = new Date();
        for (final String d : list) {
            Map m = (Map) GetFileContextUtil.getValue(d);
            String rsyncIp = m.get("rsyncIp").toString();
            String rsyncUsername = m.get("rsyncUsername").toString();
            String rsyncPassword = m.get("rsyncPassword").toString();
            String rsyncPath = m.get("rsyncPath").toString();
            String localPath = m.get("localPath").toString();
            Sync s =new Sync(rsyncIp, rsyncUsername, rsyncPassword, rsyncPath, localPath);
            R<List<File>> f = s.sync();
            System.out.println(f.ok());

        }

        Date endt = new Date();
        logger.info("结束同步时间是"+df.format(new Date()));
        logger.info( GetDate.getDatePoor(startt,endt));
        System.out.println( GetDate.getDatePoor(startt,endt));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "111";
    }




    @RequestMapping("/createPart")
    public  String createPart(int day) {
        List<String> llisttable  = null;
        try {
            llisttable = GetFileContextUtil.getFileContext("/data/config/tableList.txt");
            PrintTask task = new PrintTask();
            for (String table:llisttable){
                task.CreatePart(table, GetTime.getYesTeday(day));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "111";
    }
    @RequestMapping("/test")
    public  void test(String locafile,String hdfs) {
        File file = new File(locafile);
        new JsonToOrcWriter().write(ServerRankListLog.class, file, hdfs);
    }

}
