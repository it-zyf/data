package queue;

import java.io.File;
import java.util.concurrent.Future;

/**
 * @Author twh
 * @Date 2020/4/8 17:44
 */
public interface  ITask {
    // 上传hive
    void HdfslCmd(File fike, String table, String localPath, String hdfsPath);
    //插入impala
    void ConvertData(String table,String tabletmp,String month,String starttime);
    //创建分区
    void CreatePart(String table,String starttime);
}
