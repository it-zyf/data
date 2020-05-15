package runner;


import HdfsCmd.HdfslCmd;
import Rsync.Sync;
import hivePutFile.PutFile;
import hivePutFile.PutImpala;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import queue.PrintTask;
import queue.TaskQueue;
import runtime.ImplCmd;
import utils.*;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @Author twh
 * @Date 20200311
 */
@Component
public class TimingSync {
    //
    private final Logger logger = LoggerFactory.getLogger(getClass());
    static ExecutorService pool = Executors.newFixedThreadPool(2);
//十分鐘一次
    @Scheduled(cron = "0 */5 * * * ?")
    public void breakdownScheduleNow() throws Exception {

        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//设置日期格式
        List<String> list  = GetFileContextUtil.getFileContext("/data/config/serverip.txt");
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
    }

    @Scheduled(cron = "0 15 * ? * *")
    public void breakdownSchedule(){
        List<String> list  = null;
        try {
            list = GetFileContextUtil.getFileContext("/data/config/serverip.txt");
            System.out.println(list.size());
        String starttime= GetTime.gettime();
        for(String s:list) {
            System.out.println(s);
            PutFile.TransformationPut(s);
        }
            System.out.println("開始時間"+starttime);
            System.out.println("結束時間"+GetTime.gettime());
        } catch (Exception e) {
            logger.error(e.getMessage());
        }

    }
    @Scheduled(cron = "0 40 * ? * *")
    public void createPart(){
        List<String> llisttable  = null;
        try {
            llisttable = GetFileContextUtil.getFileContext("/data/config/tableList.txt");
            PrintTask task = new PrintTask();
            for (String table:llisttable){

                task.CreatePart(table, GetTime.getYesTeday(1));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
