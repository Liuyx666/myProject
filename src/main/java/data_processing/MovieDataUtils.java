package data_processing;

import io.grpc.proto.recommendation.MovieProfile;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.nio.charset.StandardCharsets;

public class MovieDataUtils {
    static String movies = "src\\main\\resources\\ml-25m\\movies.csv";
    static String gc = "src\\main\\resources\\ml-25m\\genome-scores.csv";
    static String ratings = "src\\main\\resources\\ml-25m\\ratings-random-80.csv";

    static List<MovieProfile.Builder> builders = new LinkedList<>();
    //movieId to its location in builders
    static Map<Integer, Integer> idToLocation = new HashMap<>();

    static double LEAST_RELEVANCE = 0.5;
    static int maxMoviesNum = 209172;
    static int[] seenTimes = new int[maxMoviesNum];
    static double[] avgRatings = new double[maxMoviesNum];
    static double[] stdDevRatings = new double[maxMoviesNum];

    public static void initData() {
        getAllRatings();
        getMovies();
        getTagIds();
        System.out.println("write start");
        for (MovieProfile.Builder builder : builders) {
            RedisUtils.writeToRedis("M-" + builder.getId(), new String(builder.build().toByteArray(), StandardCharsets.ISO_8859_1));
        }
        System.out.println("write end");
    }

    //fresh memory
    public static void freshMemory(){
        builders = null;
        idToLocation = null;
        seenTimes = null;
        avgRatings = null;
        stdDevRatings = null;
    }

    //get all ratings of movies
    private static void getAllRatings() {
        System.out.println("getAllRatings start");
        Map<Integer, List<Double>> idToRatings = new HashMap<>();
        try {
            BufferedReader reader = new BufferedReader(new FileReader(ratings));
            String lineDta;
            reader.readLine();
            while ((lineDta = reader.readLine()) != null) {
                String[] movieRatings = lineDta.split(",");
                int movieId = Integer.parseInt(movieRatings[1]);
                //get ratings
                List<Double> ratings;
                if (idToRatings.containsKey(movieId))
                    ratings = idToRatings.get(movieId);
                else
                    ratings = new LinkedList<>();
                ratings.add(Double.parseDouble(movieRatings[2]));
                idToRatings.put(movieId, ratings);
            }
            reader.close();
            //get avg_rating and std_dev_rating
            for (int i : idToRatings.keySet()) {
                List<Double> ratings = idToRatings.get(i);
                int times = ratings.size();
                seenTimes[i] = times;
                if (times > 0) {
                    avgRatings[i] = Calculate.calAVG(ratings);
                    stdDevRatings[i] = Calculate.calSTD(ratings, avgRatings[i]);
                }
            }
        } catch (FileNotFoundException e) {
            System.out.println("Not found the file!");
        } catch (IOException e) {
            System.out.println("File read/write error!");
        }
        System.out.println("getAllRatings end");
    }

    //get id, title, genre, year from movies.csv
    private static void getMovies() {
        System.out.println("getMovies start");
        try {
            BufferedReader reader = new BufferedReader(new FileReader(movies));
            String lineDta;
            reader.readLine();
            while ((lineDta = reader.readLine()) != null) {
                builders.add(generateMovie(lineDta));
            }
            reader.close();
        } catch (FileNotFoundException e) {
            System.out.println("Not found the file!");
        } catch (IOException e) {
            System.out.println("File read/write error!");
        }
        System.out.println("getMovies end");
    }

    //generate a movieProfile by data
    private static MovieProfile.Builder generateMovie(String data) {
        String[] movies = data.split(",");
        int length = movies.length;
        int movieId = Integer.parseInt(movies[0]);
        String title;
        StringBuilder titles = new StringBuilder();
        if (length <= 3)
            title = movies[1];
        else {
            int i = 1;
            while (true) {
                if (i != length - 2)
                    titles.append(movies[i]).append(",");
                else {
                    titles.append(movies[i]);
                    break;
                }
                i++;
            }
            title = titles.toString();
        }
        String year = null;
        if (movies[length - 2].length() > 6) {
            char b = movies[length - 2].charAt(movies[length - 2].length() - 2);
            char c = movies[length - 2].charAt(movies[length - 2].length() - 3);
            char d = movies[length - 2].charAt(movies[length - 2].length() - 4);
            char e = movies[length - 2].charAt(movies[length - 2].length() - 5);
            //there is a year
            if (d >= '0' && d <= '9' && e >= '0' && e <= '9') {
                if (b >= '0' && b <= '9')
                    year = movies[length - 2].substring(movies[length - 2].length() - 5, movies[length - 2].length() - 1);
                else if (c >= '0' && c <= '9')
                    year = movies[length - 2].substring(movies[length - 2].length() - 6, movies[length - 2].length() - 2);
                else
                    year = movies[length - 2].substring(movies[length - 2].length() - 7, movies[length - 2].length() - 3);
            }
        }
        //generate a builder and set attribute and return that
        MovieProfile.Builder builder = MovieProfile.newBuilder();
        idToLocation.put(movieId, builders.size());
        builder.setId(movieId).setTitle(title).setTimes(seenTimes[movieId])
                .setAvgRating(avgRatings[movieId]).setStdDev(stdDevRatings[movieId]);
        if (year != null) {
            builder.setYear(Integer.parseInt(year));
        }
        if (length >= 3)
            builder.setGenres(movies[length - 1]);
        return builder;
    }

    //get tagIds from genome-scores.csv where relevance larger LEAST_RELEVANCE
    private static void getTagIds() {
        System.out.println("getTagIds start");
        try {
            BufferedReader reader = new BufferedReader(new FileReader(gc));
            String lineDta;
            reader.readLine();
            while ((lineDta = reader.readLine()) != null) {
                String[] tagRelevance = lineDta.split(",");
                int movieId = Integer.parseInt(tagRelevance[0]);
                int tagId = Integer.parseInt(tagRelevance[1]);
                double relevance = Double.parseDouble(tagRelevance[2]);
                if (relevance >= LEAST_RELEVANCE)
                    builders.get(idToLocation.get(movieId)).addTags(tagId);
            }
            reader.close();
        } catch (FileNotFoundException e) {
            System.out.println("Not found the file!");
        } catch (IOException e) {
            System.out.println("File read/write error!");
        }
        System.out.println("getTagIds end");
    }

//    //get tagId-tag from genome-tags.csv
//    private static Map<Integer, String> dealGt(){
//        System.out.println("dealGt start");
//        //tagId to tags
//        Map<Integer, String> map = new HashMap<>();
//        try{
//            BufferedReader reader = new BufferedReader(new FileReader(gt));
//            String lineDta;
//            reader.readLine();
//            while ((lineDta = reader.readLine()) != null){
//                String[] tag = lineDta.split(",");
//                map.put(Integer.parseInt(tag[0]), tag[1]);
//            }
//            reader.close();
//        }catch (FileNotFoundException e){
//            System.out.println("Not found the file!");
//        }catch (IOException e){
//            System.out.println("File read/write error!");
//        }
//        System.out.println("dealGt end");
//        return map;
//    }
//
//    //get tags from gc, gt
//    private static void generateTags(){
//        System.out.println("generateTags start");
//        Map<Integer, List<Integer>> movieIdToTagIds = getTagIds();
//        Map<Integer, String> tagIdToTag = dealGt();
//        for (Map.Entry<Integer, List<Integer>> entry : movieIdToTagIds.entrySet()) {
//            Integer movieId = entry.getKey();
//            List<Integer> tagIds = entry.getValue();
//            for(Integer tagId : tagIds) {
//                builders.get(idToLocation.get(movieId)).addTags(tagIdToTag.get(tagId));
//            }
//        }
//        System.out.println("generateTags end");
//    }
}
