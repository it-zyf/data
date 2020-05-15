package controller;//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.hive.ql.exec.vector.*;
//import org.apache.orc.OrcFile;
//import org.apache.orc.TypeDescription;
//import org.apache.orc.Writer;


import model.bean.ServerCrossServerBossKill;
import model.bean.ServerCurrencyChange;
import model.bean.ServerPlayerCount;
import model.bean.ServerRankListLog;

import java.io.File;

public class TmpTest {
    private static final File file = new File("D:\\10203_serverCurrencyChange_2020050414_11192.json");

    public static void main(String[] args) throws Exception {
        new JsonToOrcWriterTest().write(ServerCurrencyChange.class, file, "d://");
    }


}
