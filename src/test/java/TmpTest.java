//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.hive.ql.exec.vector.*;
//import org.apache.orc.OrcFile;
//import org.apache.orc.TypeDescription;
//import org.apache.orc.Writer;


import controller.JsonToOrcWriter;
import model.bean.ServerAccountCreate;
import model.bean.ServerCrossServerBossKill;

import java.io.File;

public class TmpTest {
    private static final File file = new File("D:/10203_serverCrossServerBossKill_2020042717_15256.json");

    public static void main(String[] args) throws Exception {
        new JsonToOrcWriter().write(ServerCrossServerBossKill.class, file, file.getParent());
    }


}
