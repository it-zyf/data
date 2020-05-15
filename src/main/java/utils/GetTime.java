package utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * @Author twh
 * @Date 2020/3/31 11:24
 */
public class GetTime {
    //返回當前年月202003
    public static String getYearMOnth(int i){
        SimpleDateFormat dateFormat= new SimpleDateFormat("yyyyMM");
        Calendar cd = Calendar.getInstance();
        cd.add(Calendar.MONTH,i);
        return dateFormat.format(cd.getTime());
    }
    //返回前一個小時月份
    public static String getYearMOnthbyhour(int i){
        SimpleDateFormat dateFormat= new SimpleDateFormat("yyyyMM");
        Calendar cd = Calendar.getInstance();
        cd.add(Calendar.HOUR,i);
        return dateFormat.format(cd.getTime());
    }
    //返回前一個小時日期
    public static String getDayMOnthbyhour(int i){
        SimpleDateFormat dateFormat= new SimpleDateFormat("yyyyMMdd");
        Calendar cd = Calendar.getInstance();
        cd.add(Calendar.HOUR,i);
        return dateFormat.format(cd.getTime());
    }
    //返回當前日期年月日2020-03-31
    public static String getYearMOnthday(int i){
        SimpleDateFormat dateFormat= new SimpleDateFormat("yyyy-MM-dd");
        Calendar cd = Calendar.getInstance();
        cd.add(Calendar.HOUR,i);
        return dateFormat.format(cd.getTime());
    }
    //返回當前日期年月日2020-03-31
    public static String getYearByday(int i){
        SimpleDateFormat dateFormat= new SimpleDateFormat("yyyy-MM-dd");
        Calendar cd = Calendar.getInstance();
        cd.add(Calendar.DAY_OF_MONTH,i);
        return dateFormat.format(cd.getTime());
    }
    //返回當前日期年月日2020033115
    public static String getYearMOnthdayhour(int i){
        SimpleDateFormat dateFormat= new SimpleDateFormat("yyyyMMddHH");
        Calendar cd = Calendar.getInstance();
        cd.add(Calendar.HOUR,i);
        return dateFormat.format(cd.getTime());
    }
    //返回當前日期年月日20200331
    public static String getYesTeday(int i){
        SimpleDateFormat dateFormat= new SimpleDateFormat("yyyyMMdd");
        Calendar cd = Calendar.getInstance();
        cd.add(Calendar.DAY_OF_MONTH,i);
        return dateFormat.format(cd.getTime());
    }
    //返回當前日
    public static String gettime(){
        SimpleDateFormat dateFormat= new SimpleDateFormat("yyyy-mm-dd hh:mm:ss");
        Calendar cd = Calendar.getInstance();
        cd.add(Calendar.HOUR,0);
        return dateFormat.format(cd.getTime());
    }
    //返回當小時
    public static String gethour(int i){
        SimpleDateFormat dateFormat= new SimpleDateFormat("yyyy-MM-dd HH");
        Calendar cd = Calendar.getInstance();
        cd.add(Calendar.HOUR,i);
        return dateFormat.format(cd.getTime());
    }
    public static long timeToStamp(String timers) {
        Date d = new Date();
        long timeStemp = 0;
        try {
            SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            d = sf.parse(timers);// 日期转换为时间戳
        } catch (ParseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        timeStemp = d.getTime();
        return timeStemp;
    }
}
