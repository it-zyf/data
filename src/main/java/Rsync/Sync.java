package Rsync;


import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runtime.BiFileSync;
import utils.R;
import runtime.RuntimeResult;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class Sync {
    private static final String FILE_SUFFIX = ".json";// 需要监听的文件名后缀
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final BiFileSync fileSync;
    private final File pathDir;

    public Sync(String rsyncIp, String rsyncUsername, String rsyncPassword, String rsyncPath, String localPath) {
        this.fileSync = new BiFileSync(rsyncIp, rsyncUsername, rsyncPassword, rsyncPath, localPath);
        this.pathDir = new File(localPath);
    }

    public R<List<File>> sync() {
        final List<File> list = new ArrayList<>();
        final RuntimeResult exe = fileSync.exe();
        if (exe.isOk()) {
            final File[] files = pathDir.listFiles();
            if (null == files) {
                return R.error("同步成功，但文件列表为空");
            }
            for (File file : files) {
                final String name = file.getName();
                if (name.endsWith(FILE_SUFFIX) && 0 != file.length()) {
                    list.add(file);
                }
            }
        } else {
            return R.error(StringUtils.join(exe.getLogLines(), "\n"));
        }
        return R.ok("成功", list);
    }

    @Override
    public String toString() {
        return "Sync{" +
                "fileSync=" + fileSync +
                ", pathDir=" + pathDir +
                '}';
    }
}
