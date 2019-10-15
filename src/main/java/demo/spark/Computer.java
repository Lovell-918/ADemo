package demo.spark;

import demo.entity.*;
import scala.Tuple2;

import java.util.*;

/**
 * KDA = (k+a)/d (如果d==0则d=1)
 * 权重 = (（KDA + （cs/100 + wards）*0.6） * (1(win)|0.75(lose)) )/(场次^0.97)
 */
public class Computer {

    public void compute() {
        ReaderFromMongo readerFromMongo = new ReaderFromMongo();
        HashMap<Tuple2<String,String>, List<Game>> summonerMap = readerFromMongo.receive();
        HashMap<Tuple2<String,String>,List<ChampionAttri>> chamMap = new HashMap<>(2800);
        Iterator iterator = summonerMap.entrySet().iterator();
        HashMap<Tuple2<String,String>, Long> chamIDMap = new HashMap<>();

        while (iterator.hasNext()){
            HashMap.Entry entry = (HashMap.Entry) iterator.next();
            Tuple2<String,String> player = (Tuple2<String,String>)entry.getKey();
            List<Game> games = (List<Game>)entry.getValue();
            HashMap<String, HashMap<String,List<Double>>> championMap = new HashMap<>();
            if(games!= null) {
                long chamID  = 0;
                for (Game game : games) {
                    Tuple2<String, String> chamTul = new Tuple2<>(game.getChampion_name(),game.getPos());
                    if(!chamIDMap.containsKey(chamTul)){
                        chamID++;
                        chamIDMap.put(chamTul,chamID);
                    }
                    if(!championMap.containsKey(game.getChampion_name())){
                        HashMap<String,List<Double>> tempHm = new HashMap<>();
                        List<Double> temDouL = new ArrayList<>();
                        temDouL.add(computeWeights(game));
                        tempHm.put(game.getPos(),temDouL);
                        championMap.put(game.getChampion_name(),tempHm);
                    }else{
                        HashMap<String,List<Double>> tempHm = championMap.get(game.getChampion_name());
                        if(!tempHm.containsKey(game.getPos())){
                            List<Double> temDouL = new ArrayList<>();
                            temDouL.add(computeWeights(game));
                            tempHm.put(game.getPos(),temDouL);
                        }else{
                            List<Double> temDouL = tempHm.get(game.getPos());
                            temDouL.add(computeWeights(game));
                            tempHm.remove(game.getPos());
                            tempHm.put(game.getPos(),temDouL);
                        }
                        championMap.remove(game.getChampion_name());
                        championMap.put(game.getChampion_name(),tempHm);
                    }
                }
               Iterator cIt = championMap.entrySet().iterator();
                List<ChampionAttri> championAttriList = new ArrayList<>();
                while (cIt.hasNext()){
                    HashMap.Entry cEntry = (HashMap.Entry) cIt.next();
                    String championName = (String) cEntry.getKey();
                    HashMap<String,List<Double>> posList = (HashMap<String,List<Double>>) cEntry.getValue();
                    Iterator pIt = posList.entrySet().iterator();
                    List<PosAttri> posAttris = new ArrayList<>();
                    while (pIt.hasNext()){
                        HashMap.Entry pEntry = (HashMap.Entry) pIt.next();
                        String posString = (String)pEntry.getKey();
                        List<Double> metricList = (List<Double>)pEntry.getValue();
                        double metric = metricList.stream().mapToDouble(Double::byteValue).summaryStatistics().getSum();
                        metric = metric / Math.pow(metricList.size(),0.97);
                        posAttris.add(new PosAttri(posString,metric,chamIDMap.get(new Tuple2<>(championName,posString))));
                    }
                   championAttriList.add(new ChampionAttri(championName,posAttris));
                }
                chamMap.put(player,championAttriList);
            }
        }
        WriterInMongo writerInMongo = new WriterInMongo();
        System.out.println("数据处理完毕，开始存入Mongodb");
        writerInMongo.saveInMongo(chamMap);
    }


    private double computeWeights(Game game){
        double wei = 0;
        double d = game.getDeath() == 0? 1:game.getDeath();
        wei = (((game.getKill()+game.getAssist())/d) + (game.getCs()/(double)100 + game.getWards())*0.6);
        wei = game.getResult().equals("win") ? wei : wei*0.75;
        return wei;
    }
}
