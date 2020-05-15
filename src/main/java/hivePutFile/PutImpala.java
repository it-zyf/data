package hivePutFile;

import HdfsCmd.ConvertData;
import HdfsCmd.HdfslCmd;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import queue.PrintTask;
import utils.*;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

public class PutImpala {
    public static Logger logger = LoggerFactory.getLogger(PutImpala.class);
    public static String tableList="/data/config/tableList.txt";
    //public static String localPath="/data/log/faithtest/";




    public static void PutImpala(String starttime1) throws Exception {
        //列出所有表
        List<String> llisttable = GetFileContextUtil.getFileContext(tableList);
        System.out.println("列出所有表"+llisttable.size());
        int i=0;
        List<Future<Integer>> futures = new ArrayList<>();
        for (String table:llisttable){
            System.out.println("當前表"+table);
            if(starttime1.equals("")||starttime1==null){
                starttime1 = String.valueOf(GetTime.timeToStamp(GetTime.gethour(-1)+":00:00"));
            }
            System.out.println(starttime1);
           // ConvertData cd =new ConvertData("faithlog."+table,"faithtmp."+table,GetTime.getYearMOnthbyhour(-1),starttime1);
            PrintTask task = new PrintTask();
            task.ConvertData("faithlog."+table, "faithtmp."+table,GetTime.getDayMOnthbyhour(-1), starttime1);

        }
//        for (Future<Integer> future : futures) {
//            Integer string = (Integer) future.get();
//            System.out.println(string);
//        }

    }
}
