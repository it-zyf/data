package hivePutFile;

import HdfsCmd.ConvertData;
import HdfsCmd.HdfslCmd;
import controller.JsonToOrcWriter;
import model.bean.ServerCrossServerBossKill;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.ResourceUtils;
import queue.PrintTask;
import utils.*;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

public class PutFile {
    public static Logger logger = LoggerFactory.getLogger(PutFile.class);
    public static String tableList="/data/config/tableList.txt";
    //public static String localPath="/data/log/faithtest/";

    static String prefix = "model.bean.";
    public static void TransformationPut(String loca) throws Exception {


        Map m = (Map) GetFileContextUtil.getValue(loca);
        String serverId = m.get("serverId").toString();
        //取出本地log的目錄
        String localPath =  m.get("localPath").toString();
        List<File> filename = FileTileOrder.getFileSort(localPath+"/"+GetTime.getYearMOnthday(-1));
        System.out.println(filename.size());
        //列出所有表
        List<String> llisttable = GetFileContextUtil.getFileContext(tableList);
        for (String str : llisttable) {

            for (File fike :filename)
            {
                String className = prefix + str;
                Class<?> clz = Class.forName(className);
                className = ConvertTableName.camel4underline(clz.getSimpleName());
                if(fike.length()==0)continue;
                if(fike.getName().toLowerCase().contains(serverId+"_"+ConvertTableName.lineToHump(className).toLowerCase()+"_"+GetTime.getYearMOnthdayhour(-1).toLowerCase()))
                {
                    System.out.println("hdfs://cdh-slave1/log/faith_server/"+className+"/"+GetTime.getDayMOnthbyhour(-1));
                    System.out.println(fike.getName());
                    new JsonToOrcWriter().write(clz, fike, "hdfs://cdh-slave1/log/faith_server/" + className+"/"+GetTime.getDayMOnthbyhour(-1));
                }
            }
        }
    }
    public static void TransformationPutByday(String loca,int day) throws Exception {
        Map m = (Map) GetFileContextUtil.getValue(loca);
        String serverId = m.get("serverId").toString();
        //取出本地log的目錄
        String localPath =  m.get("localPath").toString();
        List<File> filename = FileTileOrder.getFileSort(localPath+"/"+GetTime.getYearByday(-day));
        System.out.println(filename.size());
        //列出所有表
        List<String> llisttable = GetFileContextUtil.getFileContext(tableList);
        for (String str : llisttable) {

            for (File fike :filename)
            {
                String className = prefix + str;
                Class<?> clz = Class.forName(className);
                className = ConvertTableName.camel4underline(clz.getSimpleName());
                if(fike.length()==0)continue;
                if(fike.getName().toLowerCase().contains(serverId+"_"+ConvertTableName.lineToHump(className).toLowerCase().toLowerCase())) {
                    System.out.println("hdfs://cdh-slave1/log/faith_server/" + className + "/" + GetTime.getYesTeday(-day));
                    System.out.println(fike.getName());
                    new JsonToOrcWriter().write(clz, fike, "hdfs://cdh-slave1/log/faith_server/" + className + "/" + GetTime.getYesTeday(-day));
                }
            }
        }
    }
    public static void PutHdfs(String loca) throws Exception {
        //獲取區服信息
        Map m = (Map) GetFileContextUtil.getValue(loca);
        String serverId = m.get("serverId").toString();
        //取出本地log的目錄
        String localPath = m.get("localPath").toString();
        //取出hdfs的目錄
        String hdfsPath = m.get("hdfsPath").toString();

        System.out.println("文件路徑"+localPath+"/"+GetTime.getYearMOnthday(-1));
        List<File> filename = FileTileOrder.getFileSort(localPath+"/"+GetTime.getYearMOnthday(-1));
        //列出所有表
        List<String> llisttable = GetFileContextUtil.getFileContext(tableList);
        System.out.println("列出所有表"+llisttable.size());
        int i=0;
        List<Future<Integer>> futures = new ArrayList<>();
        for (String table:llisttable){
            System.out.println("當前表"+table);
            for (File fike :filename)
            {
                if(fike.length()==0)continue;
                if(fike.getName().contains(serverId+"_"+ConvertTableName.lineToHump(table)+"_"+GetTime.getYearMOnthdayhour(-1)))
                {
                    PrintTask task = new PrintTask();
                    task.HdfslCmd(fike,table,localPath,hdfsPath);
                }
            }

        }
    }
    public static void PutHdfsDay(String loca,String starttime) throws Exception {
        //獲取區服信息
        Map m = (Map) GetFileContextUtil.getValue(loca);
        String serverId = m.get("serverId").toString();
        //取出本地log的目錄
        String localPath = m.get("localPath").toString();
        //取出hdfs的目錄
        String hdfsPath = m.get("hdfsPath").toString();

        System.out.println("文件路徑"+localPath+"/"+GetTime.getYearMOnthday(-1));
        List<File> filename = FileTileOrder.getFileSort(localPath+"/"+starttime);
        //列出所有表
        List<String> llisttable = GetFileContextUtil.getFileContext(tableList);
        System.out.println("列出所有表"+llisttable.size());
        int i=0;
        List<Future<Integer>> futures = new ArrayList<>();
        for (String table:llisttable){
            System.out.println("當前表"+table);
            for (File fike :filename)
            {
                if(fike.length()==0)continue;
                if(fike.getName().contains(serverId+"_"+ConvertTableName.lineToHump(table)))
                {
                    PrintTask task = new PrintTask();
                    task.HdfslCmd(fike,table,localPath,hdfsPath);
                }
            }

        }
    }
    public static List<String> getAllClasses(String packageName){
        List<String> classList = new ArrayList<String>();
        String className="";
        File f = new File(packageName);
        if(f.exists() && f.isDirectory()){
            File[] files = f.listFiles();
            for (File file : files) {
                className = file.getName();
                classList.add(className);
            }
            return classList;
        }else{
            logger.debug("包路径未找到！");
            return null;
        }
    }
}
