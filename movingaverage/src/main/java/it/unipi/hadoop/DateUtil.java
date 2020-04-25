package it.unipi.hadoop;

import java.util.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class DateUtil {
    static SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
    
    public static long getLongFromStringDate(String d) {
        try {
            Date date = df.parse(d); 
            long timestamp = date.getTime();
            return timestamp;
        } catch (ParseException pe) {
            pe.printStackTrace();
            return -1;
        }
    }

    public static String getStringFromLong(long l){
        Date date = new Date(l);
        return df.format(date);
    }

}