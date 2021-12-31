package data_processing;

import java.util.*;

public class Calculate {

    //calculate median
    public static double calMedian(List<Double> ratings){
        if (ratings.size() == 0)
            return 0;
        Collections.sort(ratings);
        return ratings.get(ratings.size() / 2);
    }

    //calculate average
    public static double calAVG(List<Double> ratings){
        double sum = 0;
        for (Double rating : ratings) {
            sum += rating;
        }
        return sum / ratings.size();
    }

    //calculate standard deviation
    public static double calSTD(List<Double> ratings, double avg){
        double sum = 0;
        for (Double rating : ratings) {
            sum += Math.pow(rating - avg, 2);
        }
        return Math.sqrt(sum / ratings.size());
    }

    //get the lowest value's location in the double array
    public static int getLowestLocation(double[] array){
        int lowestLocation = 0;
        for (int i = 1; i < array.length; ++ i){
            if(array[i] < array[lowestLocation])
                lowestLocation = i;
        }
        return lowestLocation;
    }

    //calculate mode
    public static List<Double> calMode(List<Double> ratings){
        Map<Double, Double> map = new HashMap<>();
        Set<Map.Entry<Double, Double>> set = map.entrySet();
        List<Double> list = new ArrayList<>();
        List<Double> listMode = new ArrayList<>();
        //统计元素出现的次数，存入Map集合
        for (double item : ratings) {
            if (!map.containsKey(item)) {
                map.put(item, 1.0);
            } else {
                map.put(item, map.get(item) + 1);
            }
        }
        //将出现的次数存入List集合
        for (Map.Entry<Double, Double> entry : set) {
            list.add(entry.getValue());
        }
        //得到最大值
        double max = calMax(list);
        //根据最大值获取众数
        for (Map.Entry<Double, Double> entry : set) {
            if (entry.getValue() == max) {
                listMode.add(entry.getKey());
            }
        }
        return listMode;
    }

    //calculate max
    public static double calMax(List<Double> ratings){
        double max = 0;
        for (Double rating : ratings) {
            if (max < rating) {
                max = rating;
            }
        }
        return max;
    }
}
