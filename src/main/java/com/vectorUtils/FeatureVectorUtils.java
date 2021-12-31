package com.vectorUtils;

import com.vectorUtils.MovieVector;

import java.util.*;

public class FeatureVectorUtils {
    public static final double avgNormalization = 2;
    public static final double yearNormalization = 0.1;
    public static final double genresNormalization = 5;

    //用欧拉公式用欧拉公式计算距离
    public static double movieDist(MovieVector movieVectorX, MovieVector movieVectorY) {
        return Math.sqrt(movieSquare(movieVectorX,movieVectorY));
    }

    //计算平方和
    public static double movieSquare(MovieVector movieVectorX, MovieVector movieVectorY) {
        return (
                (movieVectorX.getAvg() - movieVectorY.getAvg()) * avgNormalization) *
                ((movieVectorX.getAvg() - movieVectorY.getAvg()) * avgNormalization) +
//                ((movieVectorX.getYear() - movieVectorY.getYear()) * yearNormalization) *
//                        ((movieVectorX.getYear() - movieVectorY.getYear()) * yearNormalization) +
                (1 - genresSimilarity(movieVectorX.getGenres(), movieVectorY.getGenres())) * genresNormalization *
                        (1 - genresSimilarity(movieVectorX.getGenres(), movieVectorY.getGenres())) * genresNormalization * 10 +
                getTimesDiff(movieVectorX.getTimes(), movieVectorY.getTimes()) *
                        getTimesDiff(movieVectorX.getTimes(), movieVectorY.getTimes());
    }

    public static Map<Integer,Integer> getTagVecSum(Map<Integer,Integer> map1,Map<Integer,Integer> map2){
        Map<Integer,Integer> vectorMap = new HashMap<>();

        for (Map.Entry<Integer, Integer> entry : map1.entrySet()) {
            vectorMap.put(entry.getKey(),1);
        }
        for (Map.Entry<Integer, Integer> entry : map2.entrySet()) {
            if (!vectorMap.containsKey(entry.getKey())) {
                vectorMap.put(entry.getKey(),1);
            } else {
                vectorMap.put(entry.getKey(),entry.getValue() + 1);
            }
        }
        return vectorMap;
    }

    public static double genresSimilarity(Map<String,Integer> map1,Map<String,Integer> map2) {
        Map<String, int[]> vectorMap = new HashMap<>();

        for (Map.Entry<String, Integer> entry : map1.entrySet()) {
            vectorMap.put(entry.getKey(), new int[]{1, 0});
        }
        for (Map.Entry<String, Integer> entry : map2.entrySet()) {
            if (!vectorMap.containsKey(entry.getKey())) {
                vectorMap.put(entry.getKey(), new int[]{0, 1});
            } else {
                vectorMap.get(entry.getKey())[1] ++;
            }
        }

        int[] v1 = new int[vectorMap.size()];
        int[] v2 = new int[vectorMap.size()];
        int i = 0;
        for (Map.Entry<String, int[]> entry : vectorMap.entrySet()) {
            v1[i] = vectorMap.get(entry.getKey())[0];
            v2[i] = vectorMap.get(entry.getKey())[1];
            i ++;
        }
        return sim(v1,v2);
    }

    public static double tagSimilarity(Map<Integer,Integer> map1,Map<Integer,Integer> map2) {
        Map<Integer, int[]> vectorMap = new HashMap<>();
        for (Map.Entry<Integer, Integer> entry : map1.entrySet()) {
            vectorMap.put(entry.getKey(), new int[]{1, 0});
        }
        for (Map.Entry<Integer, Integer> entry : map2.entrySet()) {
            if (!vectorMap.containsKey(entry.getKey()))
                vectorMap.put(entry.getKey(), new int[]{0, 1});
            else
                vectorMap.get(entry.getKey())[1] ++;
        }
        int[] v1 = new int[vectorMap.size()];
        int[] v2 = new int[vectorMap.size()];
        int i = 0;
        for (Map.Entry<Integer, int[]> entry : vectorMap.entrySet()) {
            v1[i] = vectorMap.get(entry.getKey())[0];
            v2[i] = vectorMap.get(entry.getKey())[1];
            i ++;
        }
        return sim(v1,v2);
    }

    // 求余弦相似度
    public static double sim(int[] v1,int[] v2) {
        return pointMulti(v1,v2) / (dist(v1) * dist(v2));
    }

    // 求平方根距离
    private static double dist(int[] v) {
        double result = 0;
        for(int i : v) {
            result = result + i * i;
        }
        return Math.sqrt(result);
    }

    // 点乘法
    public static double pointMulti(int[] v1,int[] v2) {
        double result = 0;
        for(int i = 0; i < v1.length; i ++) {
            result = v1[i] * v2[i];
        }
        return result;
    }

    private static long getTimesDiff(long a, long b) {
        if (Math.abs(a - b) < 10)
            return 0;
        else if (Math.abs(a - b) < 50)
            return 1;
        else if (Math.abs(a - b) < 200)
            return 2;
        else if (Math.abs(a - b) < 500)
            return 4;
        else if (Math.abs(a - b) < 1000)
            return 6;
        else if (Math.abs(a - b) < 2500)
            return 8;
        else
            return 10;
    }
}