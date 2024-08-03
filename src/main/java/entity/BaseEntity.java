package entity;

import entity.common.BsonId;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

@Data
@NoArgsConstructor
@ToString
public class BaseEntity {
    @JsonProperty("_id")
    private BsonId id;
}
