package rating;

import com.google.protobuf.InvalidProtocolBufferException;
import data_processing.RedisUtils;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.proto.recommendation.MovieProfile;
import io.grpc.proto.recommendation.UserProfile;
import io.grpc.stub.StreamObserver;
import recommendation.MovieRecommendationGrpc;
import recommendation.Recommend;

import java.nio.charset.StandardCharsets;
import java.util.*;


public class GrpcClientPy {
    static String[] genres = {"Action", "Adventure", "Animation", "Children", "Comedy", "Crime", "Documentary",
            "Drama", "Fantasy", "Film-Noir", "Horror", "IMAX", "Musical", "Mystery", "Romance", "Sci-Fi", "Thriller", "War", "Western"};
    Map<String, MovieProfile> movies = new HashMap<>();
    static ManagedChannel managedChannel;
    static MovieRecommendationGrpc.MovieRecommendationStub stub;

    public GrpcClientPy(String host, int port) {
        managedChannel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext().build();

        stub = MovieRecommendationGrpc.newStub(managedChannel);
    }

    public Set<Integer> getRatingResult(int userId, int num, String[] movieIds) throws InvalidProtocolBufferException {
        System.out.println("-----------------------------");
        System.out.println("流式请求-响应，recommendMovies");
        Set<Integer> result = new HashSet<>();
        StreamObserver<Recommend.recReplay> moviesResponseStreamObserver = new StreamObserver<>() {
            @Override
            public void onNext(Recommend.recReplay movies) {
                String movieIds = movies.getMovieId();
                System.out.println("The response is: " + movieIds);
                //对结果movieIds-rating对分割 "," + "|" 然后调画像作为服务端传给java客户端并存csv(userId,movieId,rating)
                int count = 0;
                String[] movie_ratings = movieIds.split(",");
                for (String movie_rating : movie_ratings) {
                    result.add(Integer.parseInt(movie_rating.split("\\|")[0]));
                    count++;
                    if (count >= num)
                        break;
                }
            }

            public void onError(Throwable throwable) {
                System.out.println(throwable.getMessage());
            }

            public void onCompleted() {
                System.out.println("completed");
            }
        };
        StreamObserver<Recommend.recRequest> moviesRequestStreamObserver = stub.recommendMovies(moviesResponseStreamObserver);
        //在此处循环输入请求，请求为 作为服务器接收到java客户端传来的userId和召回后的movieId以及对应的record，至onCompleted()结束
        Map<String, Integer> genreLikeDegree = new HashMap<>();
        for (String genre : genres) {
            genreLikeDegree.put(genre, 0);
        }
        UserProfile user = UserProfile.parseFrom(RedisUtils.getValue("U-" + userId).getBytes(StandardCharsets.ISO_8859_1));
        List<Integer> seenMovies = user.getSeenMoviesList();
        List<Integer> likeMovies = user.getLikeMoviesList();
        for (Integer id : likeMovies) {
            String movieIdU = String.valueOf(id);
            MovieProfile movieU;
            if (movies.containsKey(movieIdU)) {
                movieU = movies.get(movieIdU);
            } else {
                movieU = MovieProfile.parseFrom(RedisUtils.getValue("M-" + movieIdU).getBytes(StandardCharsets.ISO_8859_1));
                movies.put(movieIdU, movieU);
            }
            String[] genresListU = movieU.getGenres().split("\\|");
            for (String genre : genresListU) {
                if (!genre.equals("(no genres listed)"))
                    genreLikeDegree.put(genre, genreLikeDegree.get(genre) + 1);
            }
        }
        StringBuilder user_record_builder = new StringBuilder();
        for (String genre : genres) {
            user_record_builder.append(",").append(genreLikeDegree.get(genre));
        }
        String user_record = user_record_builder.toString();

        for (String movieId : movieIds) {
            if(seenMovies.contains(Integer.parseInt(movieId)))
                continue;
            StringBuilder record_builder = new StringBuilder();
            record_builder.append(user.getTimes()).append(",").append(user.getMidRating()).append(",")
                    .append(String.format("%.2f", user.getAvgRating())).append(",");
            MovieProfile movie;
            if (movies.containsKey(movieId)) {
                movie = movies.get(movieId);
            } else {
                movie = MovieProfile.parseFrom(RedisUtils.getValue("M-" + movieId).getBytes(StandardCharsets.ISO_8859_1));
                movies.put(movieId, movie);
            }
            record_builder.append(String.format("%.2f", movie.getAvgRating())).append(",")
                    .append(String.format("%.4f", movie.getStdDev())).append(",").append(movie.getYear())
                    .append(",").append(movie.getTimes());
            List<String> genresList = Arrays.asList(movie.getGenres().split("\\|"));
            for (String genre : genres) {
                if (genresList.contains(genre)) {
                    record_builder.append("," + "y");
                } else {
                    record_builder.append("," + "n");
                }
            }
            record_builder.append(user_record);
            moviesRequestStreamObserver.onNext(Recommend.recRequest.newBuilder()
                    .setUserId(userId).setMovieId(Integer.parseInt(movieId)).setRecord(record_builder.toString()).build());
        }
        moviesRequestStreamObserver.onCompleted();

        try {
            //wait information from server until 50s
            Thread.sleep(4500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return result;
    }

//    public static void main(String[] args) throws InvalidProtocolBufferException {
//        GrpcClientPy client = new GrpcClientPy("192.168.43.23", 50051);
//        client.getRatingResult(1,10,new String[]{"1", "2", "3"});
//    }
}