package hivePutFile;

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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class JsonToOrcWriter {
    private static final String DATE_PATTERN = "yyyy-MM-dd HH:mm:ss";
    private static final String FILE_PATTERN = "yyyy_MM_dd_HH_mm_ss";
    private static final String DAY_PATTERN = "yyyyMMdd";

    public void write(Class<?> tClass, File inputFile, String orcPath) {
        final TypeDescription schema = TypeDescription.createStruct();
        final String fileName = DateFormatUtils.format(System.currentTimeMillis(), FILE_PATTERN);
        final List<Field> fields = new ArrayList<>(Arrays.asList(tClass.getDeclaredFields()));
        if (null != tClass.getSuperclass()) {
            fields.addAll(Arrays.asList(tClass.getSuperclass().getDeclaredFields()));
        }
        for (Field field : fields) {
            final String name = field.getName();
            final String humpToLine = CamelCaseKit.humpToLine(name);
            if (!Modifier.isStatic(field.getModifiers())) {
                final String typeName = field.getType().getTypeName();
                switch (typeName) {
                    case "java.lang.Long":
                        schema.addField(humpToLine, TypeDescription.createLong());
                        if ("logTime".equalsIgnoreCase(name)) {
                            schema.addField("date_time", TypeDescription.createString());
                            schema.addField("day_time", TypeDescription.createInt());
                        }
                        break;
                    case "java.lang.Integer":
                        schema.addField(humpToLine, TypeDescription.createInt());
                        break;
                    case "java.lang.String":
                        schema.addField(humpToLine, TypeDescription.createString());
                        break;
                    default:
                        throw new UnsupportedOperationException(typeName);
                }
            }
        }
        final Configuration conf = new Configuration();
        final String orcFile = orcPath + File.separator + fileName + "_" + RandomStringUtils.randomAlphanumeric(5) + ".orc";
        final OrcFile.WriterOptions writerOptions = OrcFile.writerOptions(conf).setSchema(schema);
        writerOptions.setProlepticGregorian(false);
        writerOptions.setShims(new HadoopShimsPre2_7());
        writerOptions.setSchema(schema)
                .overwrite(false)
                .compress(CompressionKind.SNAPPY)
                .encodingStrategy(OrcFile.EncodingStrategy.COMPRESSION)
                .version(OrcFile.Version.V_0_11);
        try (Writer writer = OrcFile.createWriter(new Path(orcFile), writerOptions); BufferedReader reader = new BufferedReader(new FileReader(inputFile))) {
            final VectorizedRowBatch batch = schema.createRowBatch();
            String line;
            while (null != (line = reader.readLine())) {
                if (line.trim().length() == 0) {
                    continue;
                }
                final JSONObject object = JSON.parseObject(line);
                final int row = batch.size++;
                int index = 0;
                for (Field field : fields) {
                    final String name = field.getName();
                    final String objStr = object.containsKey(name) ? object.get(name).toString() : null;
                    if (!Modifier.isStatic(field.getModifiers())) {
                        final String typeName = field.getType().getTypeName();
                        switch (typeName) {
                            case "java.lang.Long":
                                final LongColumnVector longColumnVector = (LongColumnVector) batch.cols[index];
                                final long longValue = StringUtils.isNotEmpty(objStr) ? Long.parseLong(objStr) : 0;
                                longColumnVector.vector[row] = longValue;
                                if ("logTime".equalsIgnoreCase(name)) {
                                    final String dateTime = DateFormatUtils.format(longValue, DATE_PATTERN);
                                    final String dayTime = DateFormatUtils.format(longValue, DAY_PATTERN);
                                    index++;
                                    final BytesColumnVector v1 = (BytesColumnVector) batch.cols[index];
                                    v1.setVal(row, (dateTime).getBytes());
                                    batchReset(batch, writer, false);
                                    index++;
                                    final LongColumnVector v2 = (LongColumnVector) batch.cols[index];
                                    v2.vector[row] = Integer.parseInt(dayTime);
                                    batchReset(batch, writer, false);
                                }
                                break;
                            case "java.lang.Integer":
                                final LongColumnVector intColumnVector = (LongColumnVector) batch.cols[index];
                                final int intValue = StringUtils.isNotEmpty(objStr) ? Integer.parseInt(objStr) : 0;
                                intColumnVector.vector[row] = intValue;
                                break;
                            case "java.lang.String":
                                final BytesColumnVector bytesColumnVector = (BytesColumnVector) batch.cols[index];
                                if (StringUtils.isNotEmpty(objStr)) {
                                    bytesColumnVector.setVal(row, (objStr).getBytes());
                                } else {
                                    bytesColumnVector.setVal(row, new byte[0]);
                                }
                                break;
                        }
                    }
                    index++;
                }
                batchReset(batch, writer, false);
            }
            batchReset(batch, writer, true);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private void batchReset(VectorizedRowBatch batch, Writer writer, boolean finish) throws IOException {
        if ((!finish && batch.size == batch.getMaxSize()) || (finish && batch.size != 0)) {
            writer.addRowBatch(batch);
            batch.reset();
        }
    }
}
