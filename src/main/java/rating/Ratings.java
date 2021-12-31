package rating;

import com.collaborativeFiltering.HotProduct;
import com.google.protobuf.InvalidProtocolBufferException;
import data_processing.MongoUtils;
import org.bson.Document;

import java.util.*;

public class Ratings {
    private static final String pyHost = "192.168.105.198";
    private static final GrpcClientPy grpcClientPy = new GrpcClientPy(pyHost, 50051);

    public static Set<Integer> getRatingResult(int userId, int num) throws InvalidProtocolBufferException {
        Document document = MongoUtils.findByConditionOne("movieRecall", "field", String.valueOf(userId));
        if (document != null) {
            String[] recMovies = document.get("value").toString().split("\\|");
            //调模型
            return grpcClientPy.getRatingResult(userId, num, recMovies);
        } else {
            //new user, random recommend 10 from 100 hot movies
            Set<Integer> hotMovies = HotProduct.hotRecommend(null, 100);
            Set<Integer> ratingResult = new HashSet<>();
            assert hotMovies != null;
            if (num >= hotMovies.size())
                ratingResult = hotMovies;
            else {
                List<Integer> recallResult = new ArrayList<>(Objects.requireNonNull(hotMovies));
                Random random = new Random();
                for (int i = 0; i < num; ++i) {
                    int location = random.nextInt(recallResult.size() - 1);
                    ratingResult.add(recallResult.get(location));
                    recallResult.remove(location);
                }
            }
            return ratingResult;
        }
    }
}
