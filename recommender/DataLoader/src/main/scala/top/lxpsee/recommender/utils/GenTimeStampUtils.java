package top.lxpsee.recommender.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Random;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/6/6 08:55.
 */
public class GenTimeStampUtils {

    public static Long getRandomDTimeStamp() {
        Calendar c = null;
        try {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            Date date = simpleDateFormat.parse("1991-01-01");

            Random random = new Random();
            c = Calendar.getInstance();
            c.setTime(date);
            c.add(Calendar.MONTH, random.nextInt(240));
            c.add(Calendar.DAY_OF_MONTH, random.nextInt(31));
            c.add(Calendar.HOUR_OF_DAY, random.nextInt(13));
            c.add(Calendar.MINUTE, random.nextInt(61));
            c.add(Calendar.SECOND, random.nextInt(61));
            c.add(Calendar.MILLISECOND, random.nextInt(1000));

            return c.getTimeInMillis();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return 0l;
    }

    public static void main(String[] args) throws ParseException {

        for (int i = 0; i < 50; i++) {
            Long time = getRandomDTimeStamp();
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:SS");
            System.out.println(simpleDateFormat.format(time));
        }

    }
}
