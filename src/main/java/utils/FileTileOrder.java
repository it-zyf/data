package utils;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.*;

public class FileTileOrder {

    /**
     * 获取目录下所有文件(按时间排序)
     *
     * @param path
     * @return
     */
    public static List<File> getFileSort(String path) {

        List<File> list = getFiles(path, new ArrayList<File>());

        if (list != null && list.size() > 0) {

            Collections.sort(list, new Comparator<File>() {
                public int compare(File file, File newFile) {
                    if (file.lastModified() > newFile.lastModified()) {
                        return 1;
                    } else if (file.lastModified() == newFile.lastModified()) {
                        return 0;
                    } else {
                        return -1;
                    }

                }
            });

        }

        return list;
    }

    /**
     *
     * 获取目录下所有文件
     *
     * @param realpath
     * @param files
     * @return
     */
    public static List<File> getFiles(String realpath, List<File> files) {

        File realFile = new File(realpath);
        if (realFile.isDirectory()) {
            File[] subfiles = realFile.listFiles();
            for (File file : subfiles) {
                if (file.isDirectory()) {
                    getFiles(file.getAbsolutePath(), files);
                } else if(file.getName().endsWith(".json")){
                    files.add(file);
                }
            }
        }
        return files;
    }

    /**
     *
     * 获取前一个小时的时间
     *

     * @return
     */
    public static String getLastTime() {
        long time =3600*1000;//60秒
        Date now = new Date();
        Date d = new Date(now.getTime() - time);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        String dateNowStr = sdf.format(d);
        return dateNowStr;
    }
    /**
     *
     * 获取本地文件路径
     *

     * @return
     */
    public static String getLocalFilePath() {
        long time =3600*1000;//60秒
        Date now = new Date();
        Date d = new Date(now.getTime() - time);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        String dateNowStr = sdf.format(d);
        return dateNowStr;
    }
    /**
     *
     * 获取hdfs文件路径
     *

     * @return
     */
    public static String getHdfsFilePath() {
        long time =3600*1000;//60秒
        Date now = new Date();
        Date d = new Date(now.getTime() - time);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        String dateNowStr = sdf.format(d);
        return dateNowStr;
    }
}
