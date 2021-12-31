package com.client;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.proto.recommendation.MovieProfile;
import io.grpc.proto.recommendation.MovieRecommendationForUserGrpc;
import io.grpc.proto.recommendation.UserRequest;

import java.util.Iterator;

public class GrpcClient {
    private static final String host = "localhost";
    private static final int port = 50051;

//    private final GreeterGrpc.GreeterBlockingStub greeterBlockingStub;
    private final MovieRecommendationForUserGrpc.MovieRecommendationForUserBlockingStub blockingStub;

    public GrpcClient() {
        ManagedChannel channel = ManagedChannelBuilder.forAddress(host,port).usePlaintext().build();
//        greeterBlockingStub = GreeterGrpc.newBlockingStub(channel);
        blockingStub = MovieRecommendationForUserGrpc.newBlockingStub(channel);
    }

//    public void greet(String name) {
//        HelloRequest request = HelloRequest.newBuilder().setName(name).build();
//        HelloReply response = greeterBlockingStub.sayHello(request);
//
//        System.out.println("Greeting: " + response.getMessage());
//    }

    public void getMovie(int id,int num)
    {
        UserRequest userRequest = UserRequest
                .newBuilder()
                .setUserId(id)
                .setMovieNum(num)
                .build();
        Iterator<MovieProfile> movieProfiles = blockingStub.baseOnUser(userRequest);

        while (movieProfiles.hasNext()) {
            MovieProfile movieProfile = movieProfiles.next();
            System.out.println(movieProfile.getId()+","+
                    movieProfile.getTitle()+","+
                    movieProfile.getGenres()+","+
                    movieProfile.getAvgRating()+","+
                    movieProfile.getStdDev()+","+
                    movieProfile.getYear()+","+
                    movieProfile.getTimes());
        }
    }

    public static void main(String[] args) {
        GrpcClient client = new GrpcClient();
        for(int i = 1 ; i < 10 ; ++ i) {
            System.out.println("正在第:" + i);
            client.getMovie(i, 5);
        }
    }
}
