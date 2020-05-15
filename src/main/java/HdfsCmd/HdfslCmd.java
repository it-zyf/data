package HdfsCmd;


import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runtime.BiFileSync;
import runtime.ImplCmd;
import runtime.RuntimeCommand;
import runtime.RuntimeResult;
import utils.R;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class HdfslCmd {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static final String hdfs = "hdfs dfs -put %s %s";
    private final String command;


    public HdfslCmd(String localPath, String hdfsLocal) {
        this.command = String.format(hdfs, localPath, hdfsLocal);
        System.out.println(this.command);
    }


    public R<List<File>> hdfs() {
        final List<File> list = new ArrayList<>();
        final RuntimeResult exe = this.exe();
        if (exe.isOk()) {
            return R.error(0,"同步成功，但文件列表为空");
        } else {
            return R.error(StringUtils.join(exe.getLogLines(), "\n"));
        }
    }
    public RuntimeResult exe() {
        final RuntimeResult result = new RuntimeCommand(command).exe();
        if (result.isOk()) {
            logger.info(StringUtils.join(result.getLogLines(), "\n"));
        } else {
            logger.error(StringUtils.join(result.getLogLines(), "\n"));
        }
        return result;
    }

}
