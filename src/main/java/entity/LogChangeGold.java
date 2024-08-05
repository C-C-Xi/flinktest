package entity;


import entity.common.BsonId;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.bson.BsonDocument;

import java.util.Date;
@Data
@NoArgsConstructor
@ToString
public class LogChangeGold  extends BaseEntity{


    @JsonProperty("PlayerId")
    private long playerId;

    @JsonProperty("Qid")
    private String qid;

    @JsonProperty("type")
    private int type;

    @JsonProperty("extType1")
    private int extType1;

    @JsonProperty("extType2")
    private int extType2;

    @JsonProperty("extType3")
    private int extType3;

    @JsonProperty("extType4")
    private int extType4;

    @JsonProperty("changeNumber")
    private int changeNumber;

    @JsonProperty("balanceNumber")
    private int balanceNumber;

    @JsonProperty("ext1")
    private int ext1;

    @JsonProperty("ext2")
    private int ext2;

    @JsonProperty("ext3")
    private int ext3;

    @JsonProperty("ext4")
    private int ext4;

    @JsonProperty("ext5")
    private long ext5;

    @JsonProperty("ext6")
    private int ext6;

    @JsonProperty("remark")
    private String remark;

    @JsonProperty("pureEarn")
    private int pureEarn;

    @JsonProperty("pureCost")
    private int pureCost;

    @JsonProperty("Time")
    private long time;

    @JsonProperty("AppType")
    private int appType;

    @JsonProperty("CountryISO2")
    private String countryISO2;

    @JsonProperty("RegisterTime")
    private long registerTime;

    @JsonProperty("changeWinningsNumber")
    private int changeWinningsNumber;

    @JsonProperty("balanceWinningsNumber")
    private int balanceWinningsNumber;

    @JsonProperty("vipLv")
    private int vipLv;

    @JsonProperty("subId")
    private int subId;

    @JsonProperty("changePGoldNumber")
    private int changePGoldNumber;

    @JsonProperty("balancePGoldNumber")
    private int balancePGoldNumber;

    @JsonProperty("payId")
    private Object payId;

    @JsonProperty("winlockTotal")
    private int winlockTotal;

    @JsonProperty("winlockProgress")
    private int winlockProgress;

    @JsonProperty("changeWinLock")
    private int changeWinLock;

    @JsonProperty("fakePGRTP")
    private int fakePGRTP;

    @JsonProperty("activityId")
    private int activityId;

    @JsonProperty("recordCount")
    private int recordCount;

    public String getKey() {
        StringBuffer stringBuffer=new StringBuffer();
        stringBuffer.append(this.playerId);
        stringBuffer.append("_");
        stringBuffer.append(this.extType1);
        stringBuffer.append("_");
        stringBuffer.append(this.extType2);
        stringBuffer.append("_");
        stringBuffer.append(this.extType3);
        stringBuffer.append("_");
        stringBuffer.append(this.extType4);
        return stringBuffer.toString();
    }
}
