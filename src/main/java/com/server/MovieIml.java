package com.server;

import com.google.protobuf.InvalidProtocolBufferException;
import data_processing.RedisUtils;
import io.grpc.proto.recommendation.MovieProfile;
import io.grpc.proto.recommendation.MovieRecommendationForUserGrpc;
import io.grpc.proto.recommendation.UserRequest;
import io.grpc.stub.StreamObserver;
import rating.GrpcClientPy;
import rating.Ratings;

import java.nio.charset.StandardCharsets;
import java.util.*;

public class MovieIml extends MovieRecommendationForUserGrpc.MovieRecommendationForUserImplBase {

    private static List<MovieProfile> fromUserToMovie(int userid, int num) {
        List<MovieProfile> movieProfileList = new ArrayList<>();
        Set<Integer> ratingResult;

        try {
            ratingResult = Ratings.getRatingResult(userid, num);
            for(int movieId : ratingResult) {
                movieProfileList.add(MovieProfile.parseFrom(RedisUtils.getValue("M-" + movieId).getBytes(StandardCharsets.ISO_8859_1)));
            }
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        return movieProfileList;
    }

    @Override
    public void baseOnUser(UserRequest userRequest, StreamObserver<MovieProfile> movieProfiles) {
        int userid = userRequest.getUserId();
        int num = userRequest.getMovieNum();

        for (MovieProfile movieProfile : fromUserToMovie(userid, num)) {
            movieProfiles.onNext(movieProfile);
        }
        movieProfiles.onCompleted();
    }
}
