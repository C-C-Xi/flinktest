package entity;

import entity.common.BsonId;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

@Data
@NoArgsConstructor
@ToString
@JsonIgnoreProperties(value = {"_id","key"})
public class BaseEntity {
    @JsonProperty("_id")
    private BsonId id;

}
