package utils;
import java.io.File;
import java.io.FileOutputStream;
import java.lang.reflect.Field;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.orc.TypeDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @Author twh
 * @Date 2020/3/26 11:36
 */
public class SqlGenerator {
    private static final Logger logger = LoggerFactory.getLogger(SqlGenerator.class);

    public static void main(String[] args) {
        //实体类所在的package在磁盘上的绝对路径
        String packageName = "F:\\datapull\\src\\main\\java\\model\\bean";
        //生成sql的文件夹
        String filePath = "F:/";
        //项目中实体类的路径
        String prefix = "model.bean.";
        String className = "";

        StringBuffer sqls = new StringBuffer();
        StringBuffer sqls1 = new StringBuffer();
        //获取包下的所有类名称
        List<String> list = getAllClasses(packageName);
        for (String str : list) {
            className = prefix + str.substring(0, str.lastIndexOf("."));
            String sql = generateSql(className, filePath);
            sqls.append(sql);
        }
        StringToSql(sqls.toString(), filePath + "report.sql");
        //StringToSql(sqls1.toString(), filePath + "reporttmp.sql");

    }
    /**
     * 根据实体类生成建表语句
     * @author
     * @date	2018年4月11日
     * @param className 全类名
     * @param filePath 磁盘路径  如 : d:/workspace/
     */

    public static String generateSql(String className,String filePath){
        try {
            Class<?> clz = Class.forName(className);
            className = ConvertTableName.camel4underline(clz.getSimpleName());
            System.out.println(clz.getSimpleName());
            Field[] fields = clz.getFields();
            StringBuffer column = new StringBuffer();

            final List<Field> fields1 = new ArrayList<>();
            if (null != clz.getSuperclass()) {
                fields1.addAll(Arrays.asList(clz.getSuperclass().getDeclaredFields()));
            }
            fields1.addAll(Arrays.asList(clz.getDeclaredFields()));


            for (Field f : fields1) {
                final String type = f.getType().getSimpleName().toLowerCase();
                final String name =  ConvertTableName.camel4underline(f.getName());
                //final String name = f.getName();
                switch (type) {
                    case "boolean":
                        column.append(name).append(" ").append("boolean,");
                        break;
                    case "integer":
                        column.append(name).append(" ").append("int,");
                        break;
                    case "int":
                        column.append(name).append(" ").append("int,");
                        break;
                    case "date":
                    case "long":
                        column.append(name).append(" ").append("bigint,");
                        if ("log_time".equalsIgnoreCase(name)) {
                            column.append("date_time").append(" ").append("string,");
                        }
                        break;
                    case "float":
                        column.append(name).append(" ").append("float,");
                        break;
                    case "double":
                        column.append(name).append(" ").append("double,");
                        break;
                    default:
                        column.append(name).append(" ").append("string,");
                        break;
                }
            }

            StringBuffer sql = new StringBuffer();
            sql.append("\n create external table if not exists `"+className+"`  (")
                    .append(" \n "+ column.substring(0, column.length()-1))
                    .append(" \n ) PARTITIONED BY (day_time int) stored as ORC location '/log/faith_server/"+className+"';");




            return sql.toString();
        } catch (ClassNotFoundException e) {
            logger.debug("该类未找到！");
            return null;
        }

    }

    public static String generateSqltmp(String className,String filePath){
        try {
            Class<?> clz = Class.forName(className);
            className = ConvertTableName.camel4underline(clz.getSimpleName());
            Field[] fields = clz.getFields();
            StringBuffer column = new StringBuffer();
            for (Field f : fields) {
                final String type = f.getType().getSimpleName().toLowerCase();
                final String name =  f.getName();
                switch (type) {
                    case "boolean":
                        column.append(name).append(" ").append("boolean,");
                        break;
                    case "integer":
                        column.append(name).append(" ").append("int,");
                        break;
                    case "int":
                        column.append(name).append(" ").append("int,");
                        break;
                    case "date":
                    case "long":
                        column.append(name).append(" ").append("bigint,");
                        break;
                    case "float":
                        column.append(name).append(" ").append("float,");
                        break;
                    case "double":
                        column.append(name).append(" ").append("double,");
                        break;
                    default:
                        column.append(name).append(" ").append("string,");
                        break;
                }
            }

            StringBuffer sql = new StringBuffer();
            sql.append("\n create external table if not exists `"+className+"`  (")
                    .append(" \n "+ column.substring(0, column.length()-1))
                    .append(" \n ) row format serde 'org.apache.hive.hcatalog.data.JsonSerDe' stored as textfile location '/log/faithtmp/"+className+"';");


            return sql.toString();
        } catch (ClassNotFoundException e) {
            logger.debug("该类未找到！");
            return null;
        }

    }
    private static String camelCase(String field) {
        final String line = String.valueOf(field.charAt(0)).toUpperCase().concat(field.substring(1));
        final Matcher matcher = Pattern.compile("[A-Z]([a-z\\d]+)?").matcher(line);
        final StringBuilder sb = new StringBuilder();
        while (matcher.find()) {
            String word = matcher.group();
            sb.append(word.toUpperCase());
            sb.append(matcher.end() == line.length() ? "" : "_");
        }
        return sb.toString().toLowerCase();
    }

    public static List<String> getAllClasses(String packageName){
        List<String> classList = new ArrayList<String>();
        String className="";
        File f = new File(packageName);
        if(f.exists() && f.isDirectory()){
            File[] files = f.listFiles();
            for (File file : files) {
                className = file.getName();
                classList.add(className);
            }
            return classList;
        }else{
            logger.debug("包路径未找到！");
            return null;
        }
    }

    public static void StringToSql(String str,String path){
        byte[] sourceByte = str.getBytes();
        if(null != sourceByte){
            try {
                File file = new File(path);     //文件路径（路径+文件名）
                if (!file.exists()) {   //文件不存在则创建文件，先创建目录
                    File dir = new File(file.getParent());
                    dir.mkdirs();
                    file.createNewFile();
                }
                FileOutputStream outStream = new FileOutputStream(file);    //文件输出流用于将数据写入文件
                outStream.write(sourceByte);
                outStream.flush();
                outStream.close();  //关闭文件输出流
                System.out.println("生成成功");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
