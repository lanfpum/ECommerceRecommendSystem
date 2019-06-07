package top.lxpsee.recommender.utils;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/6/6 08:42.
 */
public class Test4Utils {
    public static void main(String[] args) {
        long totalMilliSeconds = System.currentTimeMillis();
        long totalSeconds = totalMilliSeconds / 1000;

        //求出现在的秒
        long currentSecond = totalSeconds % 60;

        //求出现在的分
        long totalMinutes = totalSeconds / 60;
        long currentMinute = totalMinutes % 60;

        //求出现在的小时
        long totalHour = totalMinutes / 60;
        long currentHour = totalHour % 24;

        System.out.println(totalMilliSeconds);
        System.out.println(currentSecond);
        System.out.println(currentMinute);
        System.out.println(currentHour + 8);
    }
}
