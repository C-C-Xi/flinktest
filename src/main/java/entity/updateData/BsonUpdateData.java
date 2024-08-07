package entity.updateData;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
public class BsonUpdateData {


    private String updateData;
    private String condition;
    public BsonUpdateData() {

    }
    public BsonUpdateData(String updateData, String condition) {
        this.updateData = updateData;
        this.condition = condition;
    }


}
