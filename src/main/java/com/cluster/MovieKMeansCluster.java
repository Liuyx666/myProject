package com.cluster;

import com.vectorUtils.FeatureVectorUtils;
import com.vectorUtils.MovieVector;
import io.grpc.proto.recommendation.MovieProfile;

import java.util.*;

public class MovieKMeansCluster extends KMeansCluster<MovieVector,MovieProfile>{

    @Override
    public void initData(ArrayList<MovieProfile> movieProfiles) {
        for (MovieProfile movieProfile : movieProfiles) {
            MovieVector movieVector = new MovieVector(movieProfile);
//            System.out.println(movieProfile);
            dataSet.add(movieVector);
        }
    }

    @Override
    public double getTheDist(MovieVector v1, MovieVector v2) {
        return FeatureVectorUtils.movieDist(v1,v2);
    }

    @Override
    public double getTheSquare(MovieVector v1, MovieVector v2){
        return FeatureVectorUtils.movieSquare(v1,v2);
    }

    @Override
    public MovieVector setACenter(int i,int n){
        MovieVector newCenter = new MovieVector();
        for (int j = 0; j < n; j++) {
            newCenter.setAvg(cluster.get(i).get(j).getAvg() + newCenter.getAvg());
            newCenter.setYear(cluster.get(i).get(j).getYear() + newCenter.getYear());
            newCenter.setTimes(cluster.get(i).get(j).getTimes() + newCenter.getTimes());
            newCenter.setGenres(cluster.get(i).get(j).getGenres());
        }
        // 设置一个平均值
        newCenter.setAvg(newCenter.getAvg() / n);
        newCenter.setYear(newCenter.getYear() / n);
        newCenter.setTimes(newCenter.getTimes() / n);

        return newCenter;
    }

    public int[][] getClusterMovieid()
    {
        int[][] cluMov = new int[cluster.size()][];
        for(int i = 0;i < cluster.size(); i ++){
            cluMov[i] = new int[cluster.get(i).size()];
            for (int j = 0;j < cluster.get(i).size(); i ++){
                cluMov[i][j] = cluster.get(i).get(j).getId();
            }
        }
        return cluMov;
    }
}
