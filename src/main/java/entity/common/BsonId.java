package entity.common;

import lombok.Data;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

@Data
public class BsonId {
    @JsonProperty("$oid")
    private String oid;

}
