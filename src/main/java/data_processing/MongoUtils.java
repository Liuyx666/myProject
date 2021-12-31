package data_processing;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;


public class MongoUtils {
    private static final MongoClient mongoClient = new MongoClient("localhost", 27017);
    private static MongoDatabase mongoDatabase = mongoClient.getDatabase("mycol");

    public static MongoDatabase getMongoDatabase() {
        return mongoDatabase;
    }

    public static void setMongoDatabase(MongoDatabase database) {
        mongoDatabase = database;
    }

    public static void writeToMongodb(String collectionName, String field, String value) {
        MongoCollection<Document> collection = mongoDatabase.getCollection(collectionName);
        System.out.println("集合 " + collectionName + " 选择成功");

        Document document = new Document("field", field).
                append("value", value);
        collection.insertOne(document);
        System.out.println("文档插入成功");
    }

    public static MongoCollection<Document> getCollection(String collectionName) {
        MongoCollection<Document> collection = mongoDatabase.getCollection(collectionName);
        System.out.println("集合 " + collectionName + " 选择成功");

        FindIterable<Document> findIterable = collection.find();
        for (Document document : findIterable) {
            System.out.println(document);
        }

        return collection;
    }

    public static Document findByConditionOne(String collectionName,String field, String value) {
        Document whereQuery = new Document();
        whereQuery.put(field, value);
        FindIterable<Document> whereDocuments = mongoDatabase.getCollection(collectionName).find(whereQuery);
        return whereDocuments.first();
    }

    public static void main(String[] args) {
        System.out.println(getCollection("movieRecall").count());;
    }
}
