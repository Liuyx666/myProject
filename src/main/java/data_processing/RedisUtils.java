package data_processing;

import redis.clients.jedis.Jedis;

import java.text.SimpleDateFormat;
import java.util.*;

public class RedisUtils {
    private static final Jedis jedis = new Jedis("localhost");

    public RedisUtils() {
        System.out.println(jedis.ping());
    }

    public static void writeToRedis(String key, String value) {
        jedis.set(key, value);
    }

    public static String getValue(String key) {
        return jedis.get(key);
    }

    public static Set<String> getAllKeys(String type) {
        return jedis.keys(type + "-*");
    }

    public static Map<String, String> getAllKeysValues(String type) {
        Map<String, String> map = new HashMap<>();
        for (String key : getAllKeys(type)) {
            map.put(key, getValue(key));
        }
        return map;
    }

    public static List<String> getAllValues(String type) {
        List<String> values = new ArrayList<>();
        for(String key : getAllKeys(type)) {
            values.add(getValue(key));
        }
        return values;
    }

    public static void main(String[] args) {
        System.out.println(new SimpleDateFormat("dd-MM-yyyy HH:mm:ss").format(new Date()));
        jedis.flushDB();
        UserDataUtils.initData();
        UserDataUtils.freshMemory();
        System.out.println(new SimpleDateFormat("dd-MM-yyyy HH:mm:ss").format(new Date()));
        MovieDataUtils.initData();
        MovieDataUtils.freshMemory();
        System.out.println(new SimpleDateFormat("dd-MM-yyyy HH:mm:ss").format(new Date()));
    }
}
