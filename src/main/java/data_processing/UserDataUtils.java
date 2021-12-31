package data_processing;

import io.grpc.proto.recommendation.UserProfile;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class UserDataUtils {
    static String ratings = "src\\main\\resources\\ml-25m\\ratings-random-80.csv";
    //    static String tags = "src\\main\\resources\\ml-25m\\tags.csv";

    static List<UserProfile.Builder> builders = new LinkedList<>();
    //userId to its location in builders
    static Map<Integer, Integer> idToLocation = new HashMap<>();

    static int maxUsersNum = 162542;
    static int[] seenTimes = new int[maxUsersNum];
    static double[] avgRatings = new double[maxUsersNum];
    static double[] midRatings = new double[maxUsersNum];

    public static void initData() {
        getAllRatings();
        dealUsers();
//        dealTags();
        System.out.println("write start");
        for (UserProfile.Builder builder : builders) {
            RedisUtils.writeToRedis("U-" + builder.getId(), new String(builder.build().toByteArray(), StandardCharsets.ISO_8859_1));
            //todo 写入mongodb

        }
        System.out.println("write end");
    }

    //fresh memory
    public static void freshMemory(){
        builders = null;
        idToLocation = null;
        seenTimes = null;
        avgRatings = null;
        midRatings = null;
    }

    //get all ratings of users
    private static void getAllRatings(){
        System.out.println("getAllRatings start");
        Map<Integer, List<Double>> idToRatings = new HashMap<>();
        try {
            BufferedReader reader = new BufferedReader(new FileReader(ratings));
            String lineDta;
            reader.readLine();
            while ((lineDta = reader.readLine()) != null) {
                String[] userRatings = lineDta.split(",");
                int userId = Integer.parseInt(userRatings[0]);
                //get ratings
                List<Double> ratings;
                if (idToRatings.containsKey(userId))
                    ratings = idToRatings.get(userId);
                else
                    ratings = new LinkedList<>();
                ratings.add(Double.parseDouble(userRatings[2]));
                idToRatings.put(userId, ratings);
            }
            reader.close();
            //get avg_rating and mid_rating
            for (int i : idToRatings.keySet()) {
                List<Double> ratings = idToRatings.get(i);
                int times = ratings.size();
                seenTimes[i] = times;
                if (times > 0) {
                    avgRatings[i] = Calculate.calAVG(ratings);
                    midRatings[i] = Calculate.calMedian(ratings);
                }
            }
        } catch (FileNotFoundException e) {
            System.out.println("Not found the file!");
        } catch (IOException e) {
            System.out.println("File read/write error!");
        }
        System.out.println("getAllRatings end");
    }

    //get id, seen_movies, like_movies and like_ratings from ratings.csv
    private static void dealUsers() {
        System.out.println("dealUsers start");
        try {
            BufferedReader reader = new BufferedReader(new FileReader(ratings));
            String lineDta;
            reader.readLine();
            while ((lineDta = reader.readLine()) != null) {
                String[] userRatings = lineDta.split(",");
                int userId = Integer.parseInt(userRatings[0]);
                int movieId = Integer.parseInt(userRatings[1]);
                double rating = Double.parseDouble(userRatings[2]);
                UserProfile.Builder builder;
                if (idToLocation.containsKey(userId))
                    builder = builders.get(idToLocation.get(userId));
                else {
                    builder = UserProfile.newBuilder();
                    idToLocation.put(userId, builders.size());
                    builder.setId(userId).setTimes(seenTimes[userId])
                            .setAvgRating(avgRatings[userId]).setMidRating(midRatings[userId]);
                    builders.add(builder);
                }
                builder.addSeenMovies(movieId);
                //only seen some movies and then have some liked
                if(seenTimes[userId] > 0 && rating >= midRatings[userId]) {
                    builder.addLikeMovies(movieId);
                    builder.addLikeRating(rating / avgRatings[userId]);
                }
            }
            reader.close();
        } catch (FileNotFoundException e) {
            System.out.println("Not found the file!");
        } catch (IOException e) {
            System.out.println("File read/write error!");
        }
        System.out.println("dealUsers end");
    }

    //get tagged_movies, tags from tags.csv
//    private static void dealTags() {
//        System.out.println("dealTags start");
//        try {
//            BufferedReader reader = new BufferedReader(new FileReader(tags));
//            String lineDta;
//            reader.readLine();
//            while ((lineDta = reader.readLine()) != null) {
//                String[] userTags = lineDta.split(",");
//                int length = userTags.length;
//                int userId = Integer.parseInt(userTags[0]);
//                UserProfile.Builder builder;
//                if (idToLocation.containsKey(userId))
//                    builder = builders.get(idToLocation.get(userId));
//                else {
//                    builder = UserProfile.newBuilder();
//                    idToLocation.put(userId, builders.size());
//                    builder.setId(userId);
//                    builders.add(builder);
//                }
//                String tag;
//                StringBuilder taglib = new StringBuilder();
//                if (length <= 4)
//                    tag = userTags[2];
//                else {
//                    int i = 2;
//                    while (true) {
//                        if (i != length - 2)
//                            taglib.append(userTags[i]).append(",");
//                        else {
//                            taglib.append(userTags[i]);
//                            break;
//                        }
//                        i++;
//                    }
//                    tag = taglib.toString();
//                }
//                //set tagged_movies, tags
//                builder.addTaggedMovies(Integer.parseInt(userTags[1]));
//                builder.addTags(tag);
//            }
//            reader.close();
//        } catch (FileNotFoundException e) {
//            System.out.println("Not found the file!");
//        } catch (IOException e) {
//            System.out.println("File read/write error!");
//        }
//        System.out.println("dealTags end");
//    }
}
