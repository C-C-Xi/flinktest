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
import com.mongodb.client.model.ChangeStreamPreAndPostImagesOptions;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.UpdateOneModel;
import  org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import com.mongodb.client.model.UpdateOptions;
import common.ObjectMapperSingleton;
import entity.LogChangeGold;
import entity.updateData.BsonUpdateData;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.mongodb.sink.MongoSink;
import org.apache.flink.connector.mongodb.source.MongoSource;
import org.apache.flink.connector.mongodb.source.enumerator.splitter.PartitionStrategy;
import org.apache.flink.connector.mongodb.source.reader.deserializer.MongoDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.sink.AlertSink;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import ourMapFuntion.BsonToEntityMapper;
import ourReduceFuntion.LogChangeGoldReduceFuntion;
import org.apache.flink.connector.base.source.reader.RecordEmitter;

import java.util.*;

/**
 * Skeleton code for the datastream walkthrough
 */
@Slf4j
public class FraudDetectionJob {
	private static final List exchangeRefund = new ArrayList<Integer>(Arrays.asList(1005, 4001, 4002));
	public static void main(String[] args) throws Exception {
		ObjectMapper objectMapper = ObjectMapperSingleton.getInstance();
//		BroadcastStream<String> broadcastStream = logChangeGoldDataStream.broadcast();
		MongoSource<String> source = MongoSource.<String>builder()
				.setUri("mongodb://127.0.0.1:27017")
				.setDatabase("TOLog")
				.setCollection("LOG_ChangesGold")
				.setFetchSize(2048)
				.setLimit(10000)
				.setNoCursorTimeout(true)
				.setPartitionStrategy(PartitionStrategy.SAMPLE)
				.setPartitionSize(MemorySize.ofMebiBytes(64))
				.setSamplesPerPartition(10)
				.setDeserializationSchema(new MongoDeserializationSchema<String>() {
					@Override
					public String deserialize(BsonDocument document) {
						String json = document.toJson();
						return json;
					}

					@Override
					public TypeInformation<String> getProducedType() {
						return BasicTypeInfo.STRING_TYPE_INFO;
					}
				})
				.build();



		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		BsonToEntityMapper<LogChangeGold> mapper = new BsonToEntityMapper(LogChangeGold.class);

		DataStream<String> logs = env.fromSource(source, WatermarkStrategy.noWatermarks(), "MongoDB-Source");
//		logs.map(mapper).filter(logChangeGold -> {return (logChangeGold.getPlayerId()==300016210&&logChangeGold.getExtType2()==11089);}).keyBy(LogChangeGold::getKey).reduce(new LogChangeGoldReduceFuntion()).print();
		//获取游戏数据流
		DataStream<LogChangeGold> logChangeGoldDataStream= logs.map(mapper);
		// 统计玩家日数据
		DataStream<String> playerDayDataStream= logChangeGoldDataStream.map(
				new MapFunction<LogChangeGold, String>() {
			@Override
			public String map(LogChangeGold logChangeGold) throws Exception {
				Map condition=new HashMap();
				condition.put("DateTime", logChangeGold.getDateTime());
				condition.put("AppType", logChangeGold.getAppType());
				condition.put("DayStartTime", logChangeGold.getDayStartTime());


				int type = logChangeGold.getType();
				if (type == 8 && exchangeRefund.contains(logChangeGold.getExtType1())) {
					type = 99;
				}
// 获取一次性的值，避免多次调用同一方法
				long pureCost = logChangeGold.getPureCost();
				long pureEarn = logChangeGold.getPureEarn();
				long changeNumber = logChangeGold.getChangeNumber();
				Map<String, Object>setData=new HashMap();
				setData.put("endTime", logChangeGold.getTime());

				Map<String, Object>incData=new HashMap();
				incData.put("changeGold", changeNumber);
				incData.put("goldData.gold".concat(String.valueOf(type)), changeNumber);
				if(type==1){
					incData.put("gameData.pureCost", logChangeGold.getPureCost());
					incData.put("gameData.pureEarn", logChangeGold.getPureEarn());
					incData.put("gameData.getGold", changeNumber);
				}
				Map updateData=new HashMap();
				updateData.put("$set",setData);
				updateData.put("$inc",incData);

				return objectMapper.writeValueAsString(new BsonUpdateData(objectMapper.writeValueAsString(updateData),objectMapper.writeValueAsString(condition)));
			}
		});
		// 统计金币日数据
		DataStream<String> goldDayDataStream= logChangeGoldDataStream.map(
				new MapFunction<LogChangeGold, String>() {
			@Override
			public String map(LogChangeGold logChangeGold) throws Exception {
				Map condition=new HashMap();
				condition.put("DateTime", logChangeGold.getDateTime());
				condition.put("AppType", logChangeGold.getAppType());
				condition.put("DayStartTime", logChangeGold.getDayStartTime());


				int type = logChangeGold.getType();
				if (type == 8 && exchangeRefund.contains(logChangeGold.getExtType1())) {
					type = 99;
				}
// 获取一次性的值，避免多次调用同一方法
				long pureCost = logChangeGold.getPureCost();
				long pureEarn = logChangeGold.getPureEarn();
				long changeNumber = logChangeGold.getChangeNumber();
				Map<String, Object>setData=new HashMap();
				setData.put("endTime", logChangeGold.getTime());
				setData.put("balanceGold", logChangeGold.getBalanceNumber());

				Map<String, Object>incData=new HashMap();
				incData.put("goldData.gold".concat(String.valueOf(type)), changeNumber);
				if(type==1){
					incData.put("gameData.pureCost", logChangeGold.getPureCost());
					incData.put("gameData.pureEarn", logChangeGold.getPureEarn());
					incData.put("gameData.getGold", changeNumber);
					incData.put("room".concat("Gold").concat(String.valueOf(logChangeGold.getExtType2()/1000)), changeNumber);

				}
				Map updateData=new HashMap();
				updateData.put("$set",setData);
				updateData.put("$inc",incData);

				return objectMapper.writeValueAsString(new BsonUpdateData(objectMapper.writeValueAsString(updateData),objectMapper.writeValueAsString(condition)));
			}
		});
		//获取游戏数据流
		DataStream<LogChangeGold> gameDataStream=logChangeGoldDataStream.filter(logChangeGold -> logChangeGold.getType()==1);
		// 统计玩家房间日数据
		DataStream<String> playerRoomDayDataStream= gameDataStream.map(
				new MapFunction<LogChangeGold, String>() {
			@Override
			public String map(LogChangeGold logChangeGold) throws Exception {
				Map condition=new HashMap();
				condition.put("PlayerId", logChangeGold.getPlayerId());
				condition.put("stageType", logChangeGold.getExtType2());
				condition.put("DayStartTime", logChangeGold.getDayStartTime());
				condition.put("AppType", logChangeGold.getAppType());
				condition.put("DateTime", logChangeGold.getDateTime());

				int type = logChangeGold.getType();

// 获取一次性的值，避免多次调用同一方法
				long pureCost = logChangeGold.getPureCost();
				long pureEarn = logChangeGold.getPureEarn();
				long changeNumber = logChangeGold.getChangeNumber();
				Map<String, Object>setData=new HashMap();
				setData.put("endTime", logChangeGold.getTime());

				Map<String, Object>incData=new HashMap();
				incData.put("pureCost", logChangeGold.getPureCost());
				incData.put("tax", logChangeGold.getPureCost());
				incData.put("pureEarn", logChangeGold.getPureEarn());
				incData.put("getGold", changeNumber);
				Map updateData=new HashMap();
				updateData.put("$set",setData);
				updateData.put("$inc",incData);

				return objectMapper.writeValueAsString(new BsonUpdateData(objectMapper.writeValueAsString(updateData),objectMapper.writeValueAsString(condition)));
			}
		});
		// 统计房间日数据
		DataStream<String> roomDayDataStream= gameDataStream.map(
				new MapFunction<LogChangeGold, String>() {
			@Override
			public String map(LogChangeGold logChangeGold) throws Exception {
				Map condition=new HashMap();
				condition.put("stageType", logChangeGold.getExtType2());
				condition.put("DayStartTime", logChangeGold.getDayStartTime());
				condition.put("AppType", logChangeGold.getAppType());
				condition.put("DateTime", logChangeGold.getDateTime());

				int type = logChangeGold.getType();

// 获取一次性的值，避免多次调用同一方法
				long pureCost = logChangeGold.getPureCost();
				long pureEarn = logChangeGold.getPureEarn();
				long changeNumber = logChangeGold.getChangeNumber();
				Map<String, Object>setData=new HashMap();
				setData.put("endTime", logChangeGold.getTime());

				Map<String, Object>incData=new HashMap();
				incData.put("pureCost", logChangeGold.getPureCost());
				incData.put("tax", logChangeGold.getExt4());
				incData.put("pureEarn", logChangeGold.getPureEarn());
				incData.put("getGold", changeNumber);
				Map updateData=new HashMap();
				updateData.put("$set",setData);
				updateData.put("$inc",incData);

				return objectMapper.writeValueAsString(new BsonUpdateData(objectMapper.writeValueAsString(updateData),objectMapper.writeValueAsString(condition)));
			}
		});

		MongoSink<String> playerDayDataSink = MongoSink.<String>builder()
				.setUri("mongodb://127.0.0.1:27017")
				.setDatabase("flink")
				.setCollection("PlayerDayData")
				.setBatchSize(1000)
				.setBatchIntervalMs(1000)
				.setMaxRetries(3)
				.setSerializationSchema(
						(input, context) ->{
							log.info("input:{}",input);
                            BsonUpdateData map= null;
                            try {
                                map = objectMapper.readValue(input, BsonUpdateData.class);
                            } catch (JsonProcessingException e) {
								log.error("error:{}",e.getMessage());
                                throw new RuntimeException(e);
                            }
                            return new UpdateOneModel<>(BsonDocument.parse(map.getCondition()),
									BsonDocument.parse(map.getUpdateData()),new UpdateOptions().upsert(true));
							})
				.build();
		MongoSink<String>goldDayDataSink = MongoSink.<String>builder()
				.setUri("mongodb://127.0.0.1:27017")
				.setDatabase("flink")
				.setCollection("GoldDayData")
				.setBatchSize(1000)
				.setBatchIntervalMs(1000)
				.setMaxRetries(3)
				.setSerializationSchema(
						(input, context) ->{
							log.info("input:{}",input);
                            BsonUpdateData map= null;
                            try {
                                map = objectMapper.readValue(input, BsonUpdateData.class);
                            } catch (JsonProcessingException e) {
								log.error("error:{}",e.getMessage());
                                throw new RuntimeException(e);
                            }
                            return new UpdateOneModel<>(BsonDocument.parse(map.getCondition()),
									BsonDocument.parse(map.getUpdateData()),new UpdateOptions().upsert(true));
							})
				.build();
		MongoSink<String>playerRoomDayDataSink = MongoSink.<String>builder()
				.setUri("mongodb://127.0.0.1:27017")
				.setDatabase("flink")
				.setCollection("PlayerRoomDayData")
				.setBatchSize(1000)
				.setBatchIntervalMs(1000)
				.setMaxRetries(3)
				.setSerializationSchema(
						(input, context) ->{
							log.info("input:{}",input);
                            BsonUpdateData map= null;
                            try {
                                map = objectMapper.readValue(input, BsonUpdateData.class);
                            } catch (JsonProcessingException e) {
								log.error("error:{}",e.getMessage());
                                throw new RuntimeException(e);
                            }
                            return new UpdateOneModel<>(BsonDocument.parse(map.getCondition()),
									BsonDocument.parse(map.getUpdateData()),new UpdateOptions().upsert(true));
							})
				.build();
		MongoSink<String>roomDayDataSink = MongoSink.<String>builder()
				.setUri("mongodb://127.0.0.1:27017")
				.setDatabase("flink")
				.setCollection("RoomDayData")
				.setBatchSize(1000)
				.setBatchIntervalMs(1000)
				.setMaxRetries(3)
				.setSerializationSchema(
						(input, context) ->{
							log.info("input:{}",input);
                            BsonUpdateData map= null;
                            try {
                                map = objectMapper.readValue(input, BsonUpdateData.class);
                            } catch (JsonProcessingException e) {
								log.error("error:{}",e.getMessage());
                                throw new RuntimeException(e);
                            }
                            return new UpdateOneModel<>(BsonDocument.parse(map.getCondition()),
									BsonDocument.parse(map.getUpdateData()),new UpdateOptions().upsert(true));
							})
				.build();
		playerDayDataStream.sinkTo(playerDayDataSink);
		goldDayDataStream.sinkTo(goldDayDataSink);
		playerRoomDayDataStream.sinkTo(playerRoomDayDataSink);
		roomDayDataStream.sinkTo(roomDayDataSink);
		env.execute("Fraud Detection");
	}
}
