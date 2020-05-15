package runtime;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class ImplCmd {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    //    private static final String com = "sshpass -p 7ic2hjAmG7A6 rsync -avz longwang@114.116.228.127::longwang   ./agent-bi/target/serverLog";
    private static final String hdfs = "hdfs dfs -put %s %s";

    private final String command;


    public ImplCmd(String localPath, String hdfsLocal) {
        this.command = String.format(hdfs, localPath, hdfsLocal);
        System.out.println(this.command);
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

    @Override
    public String toString() {
        return "BiFileSync{" +
                "command='" + command + '\'' +
                '}';
    }
}
