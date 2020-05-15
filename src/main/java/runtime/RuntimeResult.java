package runtime;

import java.util.List;

public class RuntimeResult {
    private final boolean ok;
    private final List<String> logLines;

    public RuntimeResult(boolean ok, List<String> logLines) {
        this.ok = ok;
        this.logLines = logLines;
    }

    public boolean isOk() {
        return ok;
    }

    public List<String> getLogLines() {
        return logLines;
    }

    @Override
    public String toString() {
        return "{\"RuntimeResult\":{"
                + "\"ok\":\"" + ok + "\""
                + ", \"logLines\":" + logLines
                + "}}";
    }
}
