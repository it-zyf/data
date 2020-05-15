package runtime;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class RuntimeCommand {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final String command;

    public RuntimeCommand(String command) {
        this.command = command;
    }

    public RuntimeResult exe() {
        final List<String> logLines = new ArrayList<>();
        try {
            final Process process = Runtime.getRuntime().exec(command);
            final BufferedReader reader = new BufferedReader(new InputStreamReader(new BufferedInputStream(process.getInputStream())));
            final BufferedReader errorReader = new BufferedReader(new InputStreamReader(new BufferedInputStream(process.getErrorStream())));
            String line;
            while ((line = reader.readLine()) != null) {
                if (!StringUtils.isEmpty(line.trim())) {
                    logLines.add(line.trim());
                }
            }
            process.waitFor();
            boolean ok = process.exitValue() == 0;
            if (!ok) {
                logLines.clear();
                String error;
                while ((error = errorReader.readLine()) != null) {
                    logLines.add(error);
                }
            }
            reader.close();
            errorReader.close();
            return new RuntimeResult(ok, logLines);
        } catch (Exception e) {
            logger.error("本地指令执行失败", e);
            return new RuntimeResult(false, Collections.singletonList(e.getMessage()));
        }
    }
}
