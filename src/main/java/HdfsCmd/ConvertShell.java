package HdfsCmd;


import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runtime.RuntimeCommand;
import runtime.RuntimeResult;
import utils.R;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class ConvertShell {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static final String cmd = "sh /shell/ImplementShell.sh 'insert into %s partition (month=%s) select * from %s'";
    private final String command;


    public ConvertShell(String localPath, String hdfsLocal) {
        this.command = String.format(cmd, "serveraccountcreate1","202004", "serveraccountcreate");

        System.out.println(this.command);
    }


    public R<List<File>> convert() {
        final List<File> list = new ArrayList<>();
        final RuntimeResult exe = this.exe();
        if (exe.isOk()) {
            return R.error("執行成功");
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
