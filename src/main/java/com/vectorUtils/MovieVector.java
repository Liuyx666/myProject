package com.vectorUtils;

import io.grpc.proto.recommendation.MovieProfile;

import java.util.*;

public class MovieVector {
    private int id;
    private double avg;
    private Map<String,Integer> genres;
    private int year;
    private long times;
    private Map<Integer,Integer> tags; //tagid和数量

    public MovieVector() {
        this.genres = new HashMap<>();
        this.tags = new HashMap<>();
    }

    public MovieVector(MovieProfile movieProfile){
        this.id = movieProfile.getId();
        this.avg = movieProfile.getAvgRating();
        this.year = movieProfile.getYear();
        this.times = movieProfile.getTimes();
        initGenres(movieProfile.getGenres());
        initTags(movieProfile.getTagsList());
    }

    public MovieVector(int id,double avg, String genres, int year, long times,List<Integer> tags) {
        this.id = id;
        this.avg = avg;
        this.year = year;
        this.times = times;
        initGenres(genres);
        initTags(tags);
    }

    public void setId(int id) {
        this.id = id;
    }

    public Map<Integer, Integer> getTag() {
        return tags;
    }

    public void setTag(Map<Integer, Integer> tags) {
        this.tags = tags;
    }

    private void initTags(List<Integer> tagsId){
        this.tags = new HashMap<>();
        for (int i : tagsId) {
            this.tags.put(i,1);
        }
    }

    private void initGenres(String genres){
        this.genres = new HashMap<>();
        String[] strs = genres.split("\\|");
        for(String str : strs)
        {
            this.genres.put(str,1);
//            System.out.println(str);
        }
    }

    public double getAvg() {
        return avg;
    }

    public void setAvg(double avg) {
        this.avg = avg;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public long getTimes() {
        return times;
    }

    public void setTimes(long times) {
        this.times = times;
    }

    public Map<String,Integer> getGenres() {
        return genres;
    }

    public void setGenres(Map<String,Integer> genres) {
        for (Map.Entry<String, Integer> entry : genres.entrySet()) {
            if(!this.genres.containsKey(entry.getKey()))
                this.genres.put(entry.getKey(), 1);
            else
                this.genres.put(entry.getKey(), this.genres.get(entry.getKey()) + 1);
        }
    }

    public int getId() {
        return id;
    }

    @Override
    public String toString() {
        String str = null;

        str =  "MovieVector{\n" +
                "id=" + id +
                ", avg=" + avg +
                ", year=" + year +
                ", times=" + times;

        for(Map.Entry<String,Integer> entry: genres.entrySet()){
            str = str + "\ngenre=" + entry.getKey() + ", times=" + entry.getValue();
        }

        str = str + "\n}";

        return str;
    }
}
