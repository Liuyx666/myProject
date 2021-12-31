package com.baseContent;

import com.google.protobuf.InvalidProtocolBufferException;
import com.vectorUtils.FeatureVectorUtils;
import com.vectorUtils.MovieVector;
import data_processing.RedisUtils;
import io.grpc.proto.recommendation.MovieProfile;
import io.grpc.proto.recommendation.UserProfile;

import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.*;

public class MovieSimilarity {
    private static final double MIN_SIMI = 0.05;
    private static List<MovieVector> otherMovieVectors;
    private Map<Integer, Double> recommendList = new TreeMap<>(); //double是相似度，int是电影id
    private UserProfile userProfile;
    private List<MovieVector> userLikeMovieVectors;

    public MovieSimilarity(UserProfile userProfile) {
        this.userProfile = userProfile;
    }

    public MovieSimilarity(int uid) {
        try {
            this.userProfile = UserProfile.parseFrom(RedisUtils.getValue("U-" + uid)
                    .getBytes(StandardCharsets.ISO_8859_1));
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
    }

    private void initMovieVector(List<Integer> movieIDList) {
        System.out.println("开始初始化");
        List<String> allMovie = RedisUtils.getAllValues("M");
        userLikeMovieVectors = new ArrayList<>();
        otherMovieVectors = new ArrayList<>();
        for (String movie : allMovie) {
            try {
                MovieVector movieVector = new MovieVector(MovieProfile.parseFrom(movie.getBytes(StandardCharsets.ISO_8859_1)));
                if (movieIDList.contains(movieVector.getId())) {
                    userLikeMovieVectors.add(movieVector);
                } else {
                    otherMovieVectors.add(movieVector);
                }
//                System.out.println("添加一个movie");
            } catch (InvalidProtocolBufferException e) {
                e.printStackTrace();
            }
        }
        System.out.println("完成初始化");
    }

    public Map<Integer, Double> getRecommendList() {
        for (Map.Entry<Integer, Double> entry : recommendList.entrySet()) {
            int mapKey = entry.getKey();
            double mapValue = entry.getValue();
            System.out.println(mapKey + ":" + mapValue);
        }
        return recommendList;
    }

    private void addToRecommendList() {
        System.out.println("推荐计算");
        for (MovieVector movie: userLikeMovieVectors) {
//             System.out.println("对第"+movie.getId()+"个电影进行计算");
            for (MovieVector otherMovie : otherMovieVectors) {
                double simi = FeatureVectorUtils.tagSimilarity(movie.getTag(), otherMovie.getTag());
                if (simi > MIN_SIMI && !userLikeMovieVectors.contains(otherMovie))
                    recommendList.put(otherMovie.getId(), simi);
            }
        }
    }

    public Set<Integer> recommend() {
        System.out.println(new SimpleDateFormat("dd-MM-yyyy HH:mm:ss").format(new Date()));
        initMovieVector(userProfile.getLikeMoviesList());
        System.out.println(new SimpleDateFormat("dd-MM-yyyy HH:mm:ss").format(new Date()));
        addToRecommendList();
        System.out.println(new SimpleDateFormat("dd-MM-yyyy HH:mm:ss").format(new Date()));
        return getRecommendList().keySet();
    }
}
