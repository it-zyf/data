package queue;

import HdfsCmd.ConvertData;
import HdfsCmd.HdfslCmd;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.ConvertTableName;
import utils.GetTime;
import utils.R;

import java.io.File;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * @Author twh
 * @Date 2020/4/8 20:36
 */
public class PrintTask implements ITask {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    static ExecutorService es = Executors.newFixedThreadPool(10);

    @Override
    public void HdfslCmd(File fike,String table,String localPath, String hdfsPath) {
                    HdfslCmd cmd = new HdfslCmd(fike.toPath().toString(),hdfsPath+"/"+table+"/");
                    R<List<File>> f = cmd.hdfs();
                    logger.info(f.getMsg());
    }

    @Override
    public void ConvertData(String table, String tabletmp, String day, String starttime) {
            ConvertData cd =new ConvertData(table,tabletmp,day,starttime);
            cd.ImplConvertData();
    }

    @Override
    public void CreatePart(String table, String starttime) {
        ConvertData cd =new ConvertData(ConvertTableName.camel4underline(table),starttime);
        cd.ImplCreatePart();
    }
}
