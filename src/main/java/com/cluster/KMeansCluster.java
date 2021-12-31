package com.cluster;

import io.grpc.proto.recommendation.MovieProfile;

import java.util.*;

public abstract class KMeansCluster<T1, T2> {
    protected int k;// 分成多少簇
    protected int m;// 迭代次数
    protected int n;// 数据集元素个数，即数据集的长度
    protected List<T1> dataSet = new ArrayList<>();// 数据集链表
    protected List<T1> center;// 中心链表
    protected List<List<T1>> cluster;// 簇
    protected ArrayList<Double> jc;// 误差平方和，k越接近n，误差越小
    protected Random random;

    public void execute(ArrayList<T2> objects, int k) {
        init(objects, k);
        kmeans();
    }

    protected abstract void initData(ArrayList<T2> objects);

    /**
     * 初始化
     */
    private void init(ArrayList<T2> objects, int k) {
        this.k = k;
        this.m = 0;
        random = new Random();
        initData(objects);
        n = dataSet.size();
        initCenters();
        initCluster();
        jc = new ArrayList<>();
        System.out.println("初始化完成");
    }

    /**
     * 初始化中心数据链表，分成多少簇就有多少个中心点
     *
     * @return 中心点集
     */
    private void initCenters() {
        List<T1> center = new LinkedList<>();
        for (int i = 0; i < k; i++) {
            center.add(dataSet.get(i));
            // 生成初始化中心链表
        }
        this.center = center;
    }

    /**
     * 初始化簇集合
     *
     * @return 一个分为k簇的空数据的簇集合
     */
    private void initCluster() {
        List<List<T1>> cluster = new LinkedList<>();
        for (int i = 0; i < k; i++) {
            cluster.add(new LinkedList<>());
        }
        this.cluster = cluster;
    }

    /**
     * 获取距离集合中最小距离的位置
     *
     * @param distance 距离数组
     * @return 最小距离在距离数组中的位置
     */
    protected int minDistance(double[] distance) {
        double minDistance = distance[0];
        int minLocation = 0;
        for (int i = 1; i < distance.length; i++) {
            if (distance[i] <= minDistance) {
                minDistance = distance[i];
                minLocation = i;
            }
        }
//        System.out.println("最小位置和值"+minLocation+ " "+distance[minLocation]);
        return minLocation;
    }

    /**
     * 将当前元素放到最小距离中心相关的簇中
     */
    private void clusterSet() {
        System.out.println("放到簇中");
        for (int i = 0; i < n; i++) {
            double[] distance = new double[k];
            for (int j = 0; j < k; j++) {
                distance[j] = getTheDist(dataSet.get(i), center.get(j));
//                System.out.println("test2:" + "dataSet[" + i + "],center[" + j + "],distance=" + distance[j]);

            }
            int minLocation = minDistance(distance);
            cluster.get(minLocation).add(dataSet.get(i));
            // 将当前元素放到最小距离中心相关的簇中
        }
    }

    public abstract double getTheDist(T1 v1, T1 v2);

    /**
     * 设置新的簇中心方法
     */
    private void setNewCenter() {
        System.out.println("新找一个簇中心");
        for (int i = 0; i < k; i++) {
            int n = cluster.get(i).size();
            if (n != 0) {
                T1 newCenter = setACenter(i,n);
                center.set(i, newCenter);
            }
        }
    }

    public abstract T1 setACenter(int i,int n);

    private void termination() {
//        System.out.println("记录每次迭代的距离和");
        double jcF = 0;
        for (int i = 0; i < cluster.size(); i++) {
            for (int j = 0; j < cluster.get(i).size(); j ++) {
                jcF = jcF + getTheSquare(cluster.get(i).get(j), center.get(i));
            }
        }
        jc.add(jcF);
    }

    public abstract double getTheSquare(T1 v1, T1 v2);

    public void kmeans() {
//        System.out.println(dataSet);
//        System.out.println(cluster);
        // 循环分组，直到误差不变为止
        while (true) {
            clusterSet();
            termination();
            if (m > 1 && Objects.equals(jc.get(m - 1),jc.get(m))) {
                break;
            }
            setNewCenter();
            m++;
            initCluster();
            System.out.println("这是第" + m + "次结果" + jc.get(m - 1).toString());//输出迭代次数
            getCenter();
        }
    }

    public List<List<T1>> getCluster() {
        for (int i = 0; i < cluster.size(); i++) {
            System.out.println("簇号" + i);
            for (int j = 0; j < cluster.get(i).size(); j++)
                System.out.println(cluster.get(i).get(j).toString());
        }
        return cluster;
    }

    public List<T1> getCenter() {
        for (T1 t1 : center) {
            System.out.println(t1);
        }
        return center;
    }
}
