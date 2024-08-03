package common;

import lombok.Data;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

@Data
public class ObjectMapperSingleton {
    private static volatile ObjectMapper objectMapper = null;

    private ObjectMapperSingleton() {}

    public static ObjectMapper getInstance() {
        if (objectMapper == null) {
            synchronized (ObjectMapperSingleton.class) {
                if (objectMapper == null) {
                    objectMapper = new ObjectMapper();
                    // 配置ObjectMapper的其他设置，比如设置时间格式、忽略未知属性等
                }
            }
        }
        return objectMapper;
    }
}
