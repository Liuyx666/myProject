package com.collaborativeFiltering;

import com.google.protobuf.InvalidProtocolBufferException;
import data_processing.Calculate;
import data_processing.RedisUtils;
import io.grpc.proto.recommendation.UserProfile;

import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.*;

import static java.util.Map.Entry.comparingByValue;
import static java.util.stream.Collectors.toMap;

public class UserItemCF {
    public static Map<Integer, List<Integer>> uToMTable = new HashMap<>();
    static Map<Integer, List<Double>> uToMRTable = new HashMap<>();
    static Map<Integer, List<Integer>> mToUTable = new HashMap<>();
    static Map<Integer, List<Integer>> moviesSimilar = new HashMap<>();

    //create user likes - item table
    public static void UMTable() throws InvalidProtocolBufferException {
        System.out.println("user - item table start");
        Set<String> uIds = RedisUtils.getAllKeys("U");
        for (String uId : uIds) {
            int id = Integer.parseInt(uId.split("-")[1]);
            UserProfile userProfile = UserProfile.parseFrom(RedisUtils.getValue(uId).getBytes(StandardCharsets.ISO_8859_1));
            uToMTable.put(id, userProfile.getLikeMoviesList());
            uToMRTable.put(id, userProfile.getLikeRatingList());
        }
        System.out.println("user - item table end");
    }

    //create item - user liked table
    public static void MUTable() {
        System.out.println("item - user table start");
        for (Map.Entry<Integer, List<Integer>> entry : uToMTable.entrySet()) {
            for (Integer id : entry.getValue()) {
                if (!mToUTable.containsKey(id)) {
                    List<Integer> uIds = new LinkedList<>();
                    uIds.add(entry.getKey());
                    mToUTable.put(id, uIds);
                } else {
                    mToUTable.get(id).add(entry.getKey());
                }
            }
        }
        System.out.println("item - user table start");
    }

    //calculate every movie's most similar K movies(from all movies) ahead of schedule
    public static void getAllMostSimilarMovies(int K) {
        System.out.println("generate every movie's most similar K movies start");
        for (Integer movieId : mToUTable.keySet()) {
            moviesSimilar.put(movieId, getMostSimilarItems(movieId, K, "M"));
//            System.out.println("Movie-- " + movieId + " complete.");
        }
        System.out.println("generate every movie's most similar K movies end");
    }

    //recommend N movies to user U by K most similar items
    public static Set<Integer> recommend(int U, int N, int K, String type) throws InvalidProtocolBufferException {
        System.out.println("recommend start: user - " + U + ", movies - " + N + ", users - " + K + ", type(U or M) - " + type);

        Set<Integer> moviesResult = new HashSet<>();

        //movie id - degree
        Map<Integer, Double> recommendList = new HashMap<>();

//        //movies have been recommended
//        List<Integer> excludingList = new LinkedList<>();

        //movies have been seen can't be repeated
        List<Integer> seenMovies = UserProfile.parseFrom(RedisUtils.getValue("U-" + U)
                .getBytes(StandardCharsets.ISO_8859_1)).getSeenMoviesList();

        //all movies can be considered
        //对于UserCF，所有可以被考虑的电影是 与用户U相似的K个用户的喜欢的电影集合
        //对于ItemCF，所有可以被考虑的电影是 分别与用户U喜欢的电影相似的K个电影的集合
        Set<Integer> likeMovies = new HashSet<>();
        List<Integer> mostSimilarUsers = new LinkedList<>();
        if (type.equals("U")) {
            mostSimilarUsers = getMostSimilarItems(U, K, "U");
            likeMovies = getLikeItems(mostSimilarUsers, "U");
        } else if (type.equals("M")) {
            List<Integer> uLikeMoviesList = uToMTable.get(U);
            for (int m : uLikeMoviesList)
                likeMovies.addAll(moviesSimilar.get(m));
        } else
            System.exit(-1);

        System.out.println("generate recommend list start");
        for (int movie : likeMovies) {
            if (!seenMovies.contains(movie)) {
                double interest;
                if (type.equals("U"))
                    interest = calInterestUToM(U, movie, mostSimilarUsers);
                else {
                    interest = calInterestUToM(U, movie, K);
                }
//                System.out.println("M: " + movie + ", interest: " + interest + " put.");
                recommendList.put(movie, interest);
//                excludingList.add(movie);
            }
        }
        System.out.println("generate recommend list end");
        System.out.println(new SimpleDateFormat("dd-MM-yyyy HH:mm:ss").format(new Date()));

        LinkedHashMap<Integer, Double> reverseList = recommendList
                .entrySet()
                .stream()
                .sorted(Collections.reverseOrder(comparingByValue()))
                .collect(
                        toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e2,
                                LinkedHashMap::new));
        int count = 0;
        for (Map.Entry<Integer, Double> entry : reverseList.entrySet()) {
//            System.out.println("M: " + entry.getKey() + "---degree: " + entry.getValue());
            moviesResult.add(entry.getKey());
            if (++count > N) break;
        }
        System.out.println("recommend end");
        return moviesResult;
        //if the recommended quantity is not enough, supplement with hot
//        HotProduct.hotRecommend(excludingList, N);
    }

    //get the set of K most similar items' liked
    private static Set<Integer> getLikeItems(List<Integer> mostSimilarItems, String type) {
        System.out.println("getLikeItems start");
        Map<Integer, List<Integer>> table;
        if (type.equals("U"))
            table = uToMTable;
        else if (type.equals("M"))
            table = mToUTable;
        else {
            System.exit(-1);
            table = new HashMap<>();
        }

        Set<Integer> likeItems = new HashSet<>();
        for (int i : mostSimilarItems)
            likeItems.addAll(table.get(i));
        System.out.println("getLikeItems end");
        return likeItems;
    }

    //calculate user U's degree of interest to movie M by K most similar users
    private static double calInterestUToM(int U, int M, List<Integer> mostSimilarItems) {
        double interest = 0;
        Set<Integer> intersection = new HashSet<>();
        for (int user : mostSimilarItems)
            if (mToUTable.get(M).contains(user))
                intersection.add(user);
        for (int user : intersection) {
            //user's degree of interest to movie M
            double degree = uToMRTable.get(user).get(uToMTable.get(user).indexOf(M));
            interest += calSimilarityIToJ(U, user, "U") * degree;
        }
        return interest;
    }

    //calculate user U's degree of interest to movie M by K most similar movies
    private static double calInterestUToM(int U, int M, int K) {
        double interest = 0;
        Set<Integer> intersection = new HashSet<>();
        List<Integer> uLikeMovieList = uToMTable.get(U);
        List<Double> uLikeRatingList = uToMRTable.get(U);
        List<Integer> similarMovies = getMostSimilarItems(M, K, "M");
        for (Integer m : uLikeMovieList) {
            if (similarMovies.contains(m))
                intersection.add(m);
        }
        for (int movie : intersection) {
            //user U's degree of interest to movie movie
            double degree = uLikeRatingList.get(uLikeMovieList.indexOf(movie));
            interest += calSimilarityIToJ(M, movie, "M") * degree;
        }
        return interest;
    }

    //get K most similar items to item I (type = "U" or "M")
    private static List<Integer> getMostSimilarItems(int I, int K, String type) {
//        System.out.println("getMostSimilarItems start");
        List<Integer> mostSimilarItems = new LinkedList<>();

        double lowestSimilarity = 999;
        int lowestPosition = 0, count = 0;
        double[] mostSimilarities = new double[K];

        Map<Integer, List<Integer>> table;
        if (type.equals("U"))
            table = uToMTable;
        else if (type.equals("M"))
            table = mToUTable;
        else {
            System.exit(-1);
            table = new HashMap<>();
        }
        for (Map.Entry<Integer, List<Integer>> entry : table.entrySet()) {
            int id = entry.getKey();
            if (id == I)
                continue;
            double similarity = calSimilarityIToJ(I, id, type);
            if (count < K && similarity > 0) {
                mostSimilarities[count] = similarity;
                mostSimilarItems.add(id);
                count++;
                if (similarity < lowestSimilarity) {
                    lowestSimilarity = similarity;
                    lowestPosition = count;
                }
            } else if (similarity > lowestSimilarity) {
                mostSimilarities[lowestPosition] = similarity;
                mostSimilarItems.set(lowestPosition, id);
                lowestPosition = Calculate.getLowestLocation(mostSimilarities);
                lowestSimilarity = mostSimilarities[lowestPosition];
            }
        }
//        System.out.println("getMostSimilarItems end");
        return mostSimilarItems;
    }

    //calculate item I's similarity to item J (John.S.Breese, hit hot items) (type = "U" or "M")
    public static double calSimilarityIToJ(int I, int J, String type) {
        List<Integer> ILikes;
        List<Integer> JLikes;
        Map<Integer, List<Integer>> table;
        if (type.equals("U")) {
            ILikes = uToMTable.get(I);
            JLikes = uToMTable.get(J);
            table = mToUTable;
        } else if (type.equals("M")) {
            ILikes = mToUTable.get(I);
            JLikes = mToUTable.get(J);
            table = uToMTable;
        } else {
            System.exit(-1);
            ILikes = new LinkedList<>();
            JLikes = new LinkedList<>();
            table = new HashMap<>();
        }
        int lengthI = ILikes.size(), lengthJ = JLikes.size();
        double result = 0;
        // intersection
        if (type.equals("U")) {
            Set<Integer> intersection = new HashSet<>();
            for(Integer i : ILikes){
                if(JLikes.contains(i))
                    intersection.add(i);
            }
            for (int item : intersection) {
                result += Math.log(2) / Math.log(1 + table.get(item).size());
            }
        } else {
            ILikes.retainAll(JLikes);
            for (int item : ILikes) {
                result += Math.log(2) / Math.log(1 + table.get(item).size());
            }
        }
        double union = Math.sqrt(lengthI * lengthJ);
        return result / union;
    }
}