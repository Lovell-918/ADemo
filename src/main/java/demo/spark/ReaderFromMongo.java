package demo.spark;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import demo.entity.Game;
import demo.entity.Summoner;
import demo.mongo.MongoDBUtil;
import scala.Tuple2;

import java.util.*;

import static com.mongodb.client.model.Aggregates.project;
import static com.mongodb.client.model.Projections.*;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

public class ReaderFromMongo {

    public HashMap<Tuple2<String,String>, List<Game>> receive(){
        try {
            CodecRegistry pojoCodecRegistry = fromRegistries(MongoClientSettings.getDefaultCodecRegistry(),
                    fromProviders(PojoCodecProvider.builder().automatic(true).build()));
            MongoCollection<Summoner> gamesCollections = MongoDBUtil.getDatabase_LOL()
                    .getCollection("games", Summoner.class)
                    .withCodecRegistry(pojoCodecRegistry);

            Document filterDoc = new Document("input", "$games").append("as", "game");

            AggregateIterable<Summoner> findIterable = gamesCollections
                    .aggregate(
                            Collections.singletonList(
                                    project(
                                            fields(
                                                    excludeId(),
                                                    include("sid", "sname", "rank", "games")
                                            )
                                    )
                            )
                    );

            Iterator<Summoner> summonerIterator = findIterable.iterator();
            HashMap<Tuple2<String,String>, List<Game>> summonerMap = new HashMap<>(2800);
            Summoner summoner;
            while (summonerIterator.hasNext()) {
                summoner = summonerIterator.next();
                summonerMap.put(new Tuple2<String, String>(summoner.getSid(),summoner.getSname()),summoner.getGames());

            }
            System.out.println("数据读取完毕，开始处理");
            return summonerMap;

        }catch (Throwable t){
            t.printStackTrace();
            System.err.println("从MongoDB读取数据出错");
            return null;
        }
    }
}
