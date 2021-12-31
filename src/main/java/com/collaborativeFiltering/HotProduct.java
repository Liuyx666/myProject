package com.collaborativeFiltering;

import com.google.protobuf.InvalidProtocolBufferException;
import data_processing.RedisUtils;
import io.grpc.proto.recommendation.MovieProfile;

import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.*;

import static java.util.Map.Entry.comparingByValue;
import static java.util.stream.Collectors.toMap;

public class HotProduct {
    //item - hot degree table
    static LinkedHashMap<Integer, Double> hotMap;

    //create item - hot degree(times * avg_rating) table
    private static void IHTable() throws InvalidProtocolBufferException {
        Map<Integer, Double> iToHTable = new HashMap<>();
        System.out.println("create item - hot degree table start");
        Set<String> mIds = RedisUtils.getAllKeys("M");
        for (String mId : mIds) {
            int id = Integer.parseInt(mId.split("-")[1]);
            MovieProfile movieProfile = MovieProfile.parseFrom(RedisUtils.getValue(mId).getBytes(StandardCharsets.ISO_8859_1));
            iToHTable.put(id, movieProfile.getAvgRating() * movieProfile.getTimes());
        }
        hotMap = iToHTable
                .entrySet()
                .stream()
                .sorted(Collections.reverseOrder(comparingByValue()))
                .collect(
                        toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e2,
                                LinkedHashMap::new));
        System.out.println("create item - hot degree table end");
    }

    //recommend N - count movies by hot degree
    public static Set<Integer> hotRecommend(List<Integer> excludingList, int N) throws InvalidProtocolBufferException {
        if (excludingList != null && excludingList.size() >= N)
            return null;
        if (hotMap == null)
            IHTable();
        System.out.println("Hot products recommend start");
        System.out.println(new SimpleDateFormat("dd-MM-yyyy HH:mm:ss").format(new Date()));
        Set<Integer> moviesResult = new HashSet<>();
        int count = 0;
        if (excludingList != null)
            count = excludingList.size();
        System.out.println((N - count) + " movies will be recommended:");
        for (Map.Entry<Integer, Double> entry : hotMap.entrySet()) {
            System.out.println("M: " + entry.getKey() + "---degree: " + entry.getValue());
            moviesResult.add(entry.getKey());
            if (++count > N) break;
        }
        System.out.println("Hot products recommend end");
        System.out.println(new SimpleDateFormat("dd-MM-yyyy HH:mm:ss").format(new Date()));
        return moviesResult;
    }

    public static void main(String[] args) throws InvalidProtocolBufferException {
        System.out.println(new SimpleDateFormat("dd-MM-yyyy HH:mm:ss").format(new Date()));
        int N = 0;
        while (true) {
            System.out.println("Please input movie quantities: ");
            Scanner input = new Scanner(System.in);
            if (input.hasNextLine())
                N = Integer.parseInt(input.next());
            hotRecommend(null, N);
        }
    }
}
