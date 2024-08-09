/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package spendreport;

import common.ObjectMapperSingleton;
import common.ourMongoSink.StatisticsMongoSink;
import entity.LogChangeGold;
import entity.updateData.BsonUpdateData;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.connectors.mongodb.source.MongoDBSource;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.mongodb.source.MongoSource;
import org.apache.flink.connector.mongodb.source.enumerator.splitter.PartitionStrategy;
import org.apache.flink.connector.mongodb.source.reader.deserializer.MongoDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.bson.BsonDocument;
import ourMapFuntion.BsonToEntityMapper;

import java.util.*;

/**
 * Skeleton code for the datastream walkthrough
 */
@Slf4j
public class FraudDetectionJob {
    private static final List<Integer> exchangeRefund = new ArrayList<>(Arrays.asList(1005, 4001, 4002));

    public static void main(String[] args) throws Exception {
        ObjectMapper objectMapper = ObjectMapperSingleton.getInstance();
        MongoDBSource <String>source= MongoDBSource.<String>builder()
                .hosts("localhost:27017")
                .databaseList("TOLog") // 设置捕获的数据库，支持正则表达式
                .collectionList("TOLog.LOG_ChangesGold") //设置捕获的集合，支持正则表达式
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();
//		BroadcastStream<String> broadcastStream = logChangeGoldDataStream.broadcast();


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().enableCheckpointing(3000);

        BsonToEntityMapper<LogChangeGold> mapper = new BsonToEntityMapper<>(LogChangeGold.class);

        DataStream<String> logs = env.fromSource(source, WatermarkStrategy.forMonotonousTimestamps(), "MongoDB-Source").setParallelism(2);
//		logs.map(mapper).filter(logChangeGold -> {return (logChangeGold.getPlayerId()==300016210&&logChangeGold.getExtType2()==11089);}).keyBy(LogChangeGold::getKey).reduce(new LogChangeGoldReduceFuntion()).print();
        //获取游戏数据流
        DataStream<LogChangeGold> logChangeGoldDataStream = logs.map(mapper).setParallelism(2);
        // 统计玩家日数据
        DataStream<String> playerDayDataStream = logChangeGoldDataStream.map((MapFunction<LogChangeGold, String>) logChangeGold -> {
            Map<String, Object> condition = new HashMap<>();
            condition.put("PlayerId", logChangeGold.getPlayerId());
            condition.put("DateTime", logChangeGold.getDateTime());
            condition.put("AppType", logChangeGold.getAppType());
            condition.put("DayStartTime", logChangeGold.getDayStartTime());

            condition.put("Qid", logChangeGold.getQid());


            int type = logChangeGold.getType();
            if (type == 8 && exchangeRefund.contains(logChangeGold.getExtType1())) {
                type = 99;
            }
// 获取一次性的值，避免多次调用同一方法
            long changeNumber = logChangeGold.getChangeNumber();
            Map<String, Object> setData = new HashMap<>();
            setData.put("endTime", logChangeGold.getTime());
            setData.put("newUser", logChangeGold.getNewUser());
            setData.put("vipLv", logChangeGold.getVipLv());
            setData.put("balanceGold", logChangeGold.getBalanceNumber());
            Map<String, Object> incData = new HashMap<>();
            incData.put("changeGold", changeNumber);
            incData.put("goldData.gold".concat(String.valueOf(type)), changeNumber);
            if (type == 1) {
                incData.put("gameData.pureCost", logChangeGold.getPureCost());
                incData.put("gameData.pureEarn", logChangeGold.getPureEarn());
                incData.put("gameData.getGold", changeNumber);
            }
            Map<String, Object> updateData = new HashMap<>();
            updateData.put("$set", setData);
            updateData.put("$inc", incData);

            return objectMapper.writeValueAsString(new BsonUpdateData(objectMapper.writeValueAsString(updateData), objectMapper.writeValueAsString(condition)));
        });
        // 统计金币日数据
        DataStream<String> goldDayDataStream = logChangeGoldDataStream.map((MapFunction<LogChangeGold, String>) logChangeGold -> {
            Map<String, Object> condition = new HashMap<>();
            condition.put("DateTime", logChangeGold.getDateTime());
            condition.put("AppType", logChangeGold.getAppType());
            condition.put("DayStartTime", logChangeGold.getDayStartTime());
            condition.put("Qid", logChangeGold.getQid());

            int type = logChangeGold.getType();
            if (type == 8 && exchangeRefund.contains(logChangeGold.getExtType1())) {
                type = 99;
            }
// 获取一次性的值，避免多次调用同一方法
            long pureCost = logChangeGold.getPureCost();
            long pureEarn = logChangeGold.getPureEarn();
            long changeNumber = logChangeGold.getChangeNumber();
            Map<String, Object> setData = new HashMap<>();
            setData.put("endTime", logChangeGold.getTime());

            Map<String, Object> incData = new HashMap<>();
            incData.put("goldData.gold".concat(String.valueOf(type)), changeNumber);
            if (type == 1) {
                incData.put("gameData.pureCost", pureCost);
                incData.put("gameData.pureEarn", pureEarn);
                incData.put("gameData.getGold", changeNumber);
                incData.put("room".concat("Gold").concat(String.valueOf(logChangeGold.getExtType2() / 1000)), changeNumber);

            }
            Map<String, Object> updateData = new HashMap<>();
            updateData.put("$set", setData);
            updateData.put("$inc", incData);

            return objectMapper.writeValueAsString(new BsonUpdateData(objectMapper.writeValueAsString(updateData), objectMapper.writeValueAsString(condition)));
        });
        //获取游戏数据流
        DataStream<LogChangeGold> gameDataStream = logChangeGoldDataStream.filter(logChangeGold -> logChangeGold.getType() == 1);
        // 统计玩家房间日数据
        DataStream<String> playerRoomDayDataStream = gameDataStream.map((MapFunction<LogChangeGold, String>) logChangeGold -> {
            Map<String, Object> condition = new HashMap<>();
            condition.put("PlayerId", logChangeGold.getPlayerId());
            condition.put("StageType", logChangeGold.getExtType2());
            condition.put("DayStartTime", logChangeGold.getDayStartTime());
            condition.put("AppType", logChangeGold.getAppType());
            condition.put("DateTime", logChangeGold.getDateTime());
            condition.put("Qid", logChangeGold.getQid());

            long pureCost = logChangeGold.getPureCost();
            long pureEarn = logChangeGold.getPureEarn();
            long changeNumber = logChangeGold.getChangeNumber();
            Map<String, Object> setData = new HashMap<>();
            setData.put("endTime", logChangeGold.getTime());

            setData.put("newUser", logChangeGold.getNewUser());
            setData.put("vipLv", logChangeGold.getVipLv());
            Map<String, Object> incData = new HashMap<>();
            incData.put("pureCost", pureCost);
            incData.put("tax", logChangeGold.getPureCost());
            incData.put("pureEarn", pureEarn);
            incData.put("getGold", changeNumber);
            Map<String, Object> updateData = new HashMap<>();
            updateData.put("$set", setData);
            updateData.put("$inc", incData);

            return objectMapper.writeValueAsString(new BsonUpdateData(objectMapper.writeValueAsString(updateData), objectMapper.writeValueAsString(condition)));
        });
        // 统计房间日数据
        DataStream<String> roomDayDataStream = gameDataStream.map((MapFunction<LogChangeGold, String>) logChangeGold -> {
            Map<String, Object> condition = new HashMap<>();
            condition.put("stageType", logChangeGold.getExtType2());
            condition.put("DayStartTime", logChangeGold.getDayStartTime());
            condition.put("AppType", logChangeGold.getAppType());
            condition.put("DateTime", logChangeGold.getDateTime());
            long pureCost = logChangeGold.getPureCost();
            long pureEarn = logChangeGold.getPureEarn();
            long changeNumber = logChangeGold.getChangeNumber();
            Map<String, Object> setData = new HashMap<>();
            setData.put("endTime", logChangeGold.getTime());

            Map<String, Object> incData = new HashMap<>();
            incData.put("pureCost",pureCost);
            incData.put("tax", logChangeGold.getExt4());
            incData.put("pureEarn", pureEarn);
            incData.put("getGold", changeNumber);
            Map<String, Object> updateData = new HashMap<>();
            updateData.put("$set", setData);
            updateData.put("$inc", incData);

            return objectMapper.writeValueAsString(new BsonUpdateData(objectMapper.writeValueAsString(updateData), objectMapper.writeValueAsString(condition)));
        });

        playerDayDataStream.sinkTo(StatisticsMongoSink.getMongoSink( StatisticsMongoSink.COLLECTION_PLAYER_DAY_DATA));
        goldDayDataStream.sinkTo(StatisticsMongoSink.getMongoSink( StatisticsMongoSink.COLLECTION_GOLD_DAY_DATA));
        playerRoomDayDataStream.sinkTo(StatisticsMongoSink.getMongoSink( StatisticsMongoSink.COLLECTION_PLAYER_ROOM_DAY_DATA));
        roomDayDataStream.sinkTo(StatisticsMongoSink.getMongoSink( StatisticsMongoSink.COLLECTION_ROOM_DAY_DATA));
        env.execute("Fraud Detection");
    }
}
