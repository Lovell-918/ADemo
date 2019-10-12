package demo.spark;

import com.mongodb.MongoClientSettings;
import demo.entity.ChampionAttri;
import demo.entity.PosAttri;
import demo.mongo.MongoDBUtil;
import org.bson.BsonArray;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;


public class WriterInMongo {
    public void saveInMongo(HashMap<Tuple2<String,String>,List<ChampionAttri>> chamMap){

        try {

            CodecRegistry pojoCodecRegistry = fromRegistries(MongoClientSettings.getDefaultCodecRegistry(),
                    fromProviders(PojoCodecProvider.builder().automatic(true).build()));

            MongoDBUtil.creatCollection("base_graph");
            List<Document> documents = new ArrayList<>();
            Iterator it = chamMap.entrySet().iterator();
            while (it.hasNext()){
                HashMap.Entry entry = (HashMap.Entry) it.next();
                Tuple2<String,String> player = (Tuple2<String,String>) entry.getKey();
                List<ChampionAttri> championAttriList = (List<ChampionAttri>) entry.getValue();
                BsonArray bsonChampion = new BsonArray();
                for(ChampionAttri championAttri:championAttriList){
                    String chamName = championAttri.getChampion_name();
                    List<PosAttri> posAttriList = championAttri.getPosAttriList();
                    BsonArray bsonPos = new BsonArray();
                    for(PosAttri posAttri:posAttriList){
                        Document documentPos = new Document().append("pos",posAttri.getPos()).append("metric",posAttri.getMetric());
                        bsonPos.add(documentPos.toBsonDocument(PosAttri.class,pojoCodecRegistry));
                    }
                    Document documentCham = new Document().append("champion_name",chamName).append("pos_list",bsonPos);
                    bsonChampion.add(documentCham.toBsonDocument(ChampionAttri.class,pojoCodecRegistry));
                }
                Document document = new Document("sid",player._1()).append("sname",player._2()).append("championAttriList",bsonChampion);
                documents.add(document);
            }

            MongoDBUtil.getDatabase_LOL()
                    .getCollection("base_graph").insertMany(documents);
            System.out.println("写入Mongodb完成");


        }catch (Throwable t){
            t.printStackTrace();
            System.err.println("写入Mongodb失败");
        }

    }
}
