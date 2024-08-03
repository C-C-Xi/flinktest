package ourMapFuntion;

import common.ObjectMapperSingleton;
import entity.LogChangeGold;
import lombok.Data;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

@Data
public class BsonToEntityMapper<O> implements MapFunction<String,O> {
    private Class<O> type;

    public BsonToEntityMapper(Class<O> type) {
        this.type = type;
    }


    @Override
    public O map(String t) throws Exception {
        ObjectMapper objectMapper = ObjectMapperSingleton.getInstance();
        return objectMapper.readValue(t,type);
    }
}
