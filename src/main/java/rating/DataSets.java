package rating;

import data_processing.RedisUtils;
import io.grpc.proto.recommendation.MovieProfile;
import io.grpc.proto.recommendation.UserProfile;


import java.io.*;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.*;

public class DataSets {
    static String training = "src\\main\\resources\\ml-25m\\ratings-random-80-80.csv";
    static String adjust = "src\\main\\resources\\ml-25m\\ratings-random-80-20.csv";
    static String trainingDataSets = "src\\main\\resources\\ml-25m\\training.csv";
    static String adjustDataSets = "src\\main\\resources\\ml-25m\\adjust.csv";
    static String[] genres = {"Action", "Adventure", "Animation", "Children", "Comedy", "Crime", "Documentary",
            "Drama", "Fantasy", "Film-Noir", "Horror", "IMAX", "Musical", "Mystery", "Romance", "Sci-Fi", "Thriller", "War", "Western"};

    public static void main(String[] args) {
        System.out.println(new SimpleDateFormat("dd-MM-yyyy HH:mm:ss").format(new Date()));
        generateDataSets(training, trainingDataSets);
        System.out.println(new SimpleDateFormat("dd-MM-yyyy HH:mm:ss").format(new Date()));
        generateDataSets(adjust, adjustDataSets);
        System.out.println(new SimpleDateFormat("dd-MM-yyyy HH:mm:ss").format(new Date()));
    }

    private static void generateDataSets(String src, String des) {
        Map<String, UserProfile> users = new HashMap<>();
        Map<String, MovieProfile> movies = new HashMap<>();
//        Map<String, Integer> genreLikeDegree = new HashMap<>();
//        for (String genre : genres) {
//            genreLikeDegree.put(genre, 0);
//        }
        System.out.println("generateDataSets start - " + src);
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(des));
            writer.write("Like,U-Times,U-MidRating,U-AvgRating,M-AvgRating,M-StdDev,M-Year,M-Times,M-Genres");

            BufferedReader reader = new BufferedReader(new FileReader(src));
            String lineDta;
            reader.readLine();
            while ((lineDta = reader.readLine()) != null) {
                String[] record = lineDta.split(",");
                String userId = record[0];
                String movieId = record[1];
                String like;

                UserProfile user;
                MovieProfile movie;
                if (users.containsKey(userId)) {
                    user = users.get(userId);
                } else {
                    user = UserProfile.parseFrom(RedisUtils.getValue("U-" + userId).getBytes(StandardCharsets.ISO_8859_1));
                    users.put(userId, user);
                }
                if (movies.containsKey(movieId)) {
                    movie = movies.get(movieId);
                } else {
                    movie = MovieProfile.parseFrom(RedisUtils.getValue("M-" + movieId).getBytes(StandardCharsets.ISO_8859_1));
                    movies.put(movieId, movie);
                }

                if (Double.parseDouble(record[2]) >= 3.5 || (Double.parseDouble(record[2]) >= 2 && Double.parseDouble(record[2]) >= user.getMidRating()))
                    like = "1";
                else
                    like = "0";

                writer.newLine();
                writer.write(like + "," + user.getTimes() + "," + user.getMidRating() + "," + String.format("%.2f", user.getAvgRating())
                        + "," + String.format("%.2f", movie.getAvgRating()) + "," + String.format("%.4f", movie.getStdDev())
                        + "," + movie.getYear() + "," + movie.getTimes() + ',' + movie.getGenres());
//                List<String> genresList = Arrays.asList(movie.getGenres().split("\\|"));
//                for (String genre : genres) {
//                    if (genresList.contains(genre)) {
//                        writer.write("," + "y");
//                    } else {
//                        writer.write("," + "n");
//                    }
//                }
//                List<Integer> likeMovies = user.getLikeMoviesList();
//                for(Integer id : likeMovies){
//                    String movieIdU = String.valueOf(id);
//                    MovieProfile movieU;
//                    if (movies.containsKey(movieIdU)) {
//                        movieU = movies.get(movieIdU);
//                    } else {
//                        movieU = MovieProfile.parseFrom(RedisUtils.getValue("M-" + movieIdU).getBytes(StandardCharsets.ISO_8859_1));
//                        movies.put(movieIdU, movieU);
//                    }
//                    String[] genresListU = movieU.getGenres().split("\\|");
//                    for(String genre : genresListU) {
//                        if(!genre.equals("(no genres listed)"))
//                            genreLikeDegree.put(genre, genreLikeDegree.get(genre) + 1);
//                    }
//                }
//                for(String genre : genres){
//                    writer.write("," + genreLikeDegree.get(genre));
//                    genreLikeDegree.put(genre, 0);
//                }
            }
            writer.flush();
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("generateDataSets end - " + des);
    }
}
