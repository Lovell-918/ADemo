package demo.mongo;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;

public class MongoDBUtil {
    private static MongoClient mongoClient = null;
    private static MongoDatabase Database_LOL = null;

    public static MongoClient getMongoClient() {
        if (mongoClient == null) {
            mongoClient = MongoClients.create();
        }
        return mongoClient;
    }

    public static MongoDatabase getDatabase_LOL() {
        if (Database_LOL == null) {
            Database_LOL = getMongoClient().getDatabase("lol");
        }
        return Database_LOL;
    }

    public static <T> MongoCollection<T> getCollection(String name, Class<T> tClass) {
        final CodecRegistry codecRegistry = CodecRegistries
                .fromRegistries(
                        MongoClientSettings.getDefaultCodecRegistry(),
                        CodecRegistries.fromProviders(PojoCodecProvider.builder().automatic(true).build())
                );
        return getDatabase_LOL().getCollection(name, tClass).withCodecRegistry(codecRegistry);
    }

    public static void creatCollection(String name){
        getDatabase_LOL().createCollection(name);

    }
}