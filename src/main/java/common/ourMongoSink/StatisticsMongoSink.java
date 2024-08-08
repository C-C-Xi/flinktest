package common.ourMongoSink;

import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import common.ObjectMapperSingleton;
import entity.updateData.BsonUpdateData;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.connector.mongodb.sink.MongoSink;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.bson.BsonDocument;


@Data
@Slf4j
public class StatisticsMongoSink {
    public static final String COLLECTION_PLAYER_DAY_DATA = "PlayerDayData";
    public static final String COLLECTION_GOLD_DAY_DATA = "GoldDayData";
    public static final String COLLECTION_PLAYER_ROOM_DAY_DATA= "PlayerRoomDayData";
    public static final String COLLECTION_ROOM_DAY_DATA = "RoomDayData";

    public  static MongoSink<String> getMongoSink( String collection) {
        ObjectMapper objectMapper = ObjectMapperSingleton.getInstance();
        return MongoSink.<String>builder()
                .setUri("mongodb://127.0.0.1:27017")
                .setDatabase("flink")
                .setCollection(collection)
                .setBatchSize(1000)
                .setBatchIntervalMs(1000)
                .setMaxRetries(3)
                .setSerializationSchema(
                        (input, context) ->{
                            log.info("input:{}",input);
                            BsonUpdateData map;
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
    }
}
