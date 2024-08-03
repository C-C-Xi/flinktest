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

import entity.LogChangeGold;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.mongodb.source.MongoSource;
import org.apache.flink.connector.mongodb.source.enumerator.splitter.PartitionStrategy;
import org.apache.flink.connector.mongodb.source.reader.deserializer.MongoDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.sink.AlertSink;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import ourMapFuntion.BsonToEntityMapper;

/**
 * Skeleton code for the datastream walkthrough
 */
@Slf4j
public class FraudDetectionJob {
	public static void main(String[] args) throws Exception {
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
						log.info("deserialize{}",json);
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
		DataStream<Alert> alertDataStream= logs.map(mapper).keyBy(LogChangeGold::getPlayerId).process(new ChangeGoldStatistics()).name("ChangeGoldStatistics");
        alertDataStream.addSink(new AlertSink())
				.name("send-alerts");
		env.execute("Fraud Detection");
	}
}
