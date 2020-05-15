package controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.impl.HadoopShimsPre2_7;
import utils.CamelCaseKit;

import java.io.*;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class JsonToOrcWriterTest {

    public void write(Class<?> tClass, File inputFile, String orcPath) throws IOException {
        final TypeDescription schema = TypeDescription.createStruct();
        final List<Field> fields = new ArrayList<>();
        if (null != tClass.getSuperclass()) {
            fields.addAll(Arrays.asList(tClass.getSuperclass().getDeclaredFields()));
        }
        fields.addAll(Arrays.asList(tClass.getDeclaredFields()));
        for (Field field : fields) {
            final String name = field.getName();
            //final String humpToLine = CamelCaseKit.humpToLine(name);
            BufferedReader reader = new BufferedReader(new FileReader(inputFile));
            final JSONObject object = JSON.parseObject(reader.readLine());
            if(!object.containsKey(name)) System.out.println(name);
            }
        }

}
