package entity;


import common.LocalDateTimeConverter;
import entity.common.BsonId;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.bson.BsonDocument;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Objects;

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

    @JsonProperty("dateTime")
    private int dateTime;

    @JsonProperty("dayStartTime")
    private long dayStartTime;

    @JsonProperty("roomType")
    private int roomType;

    public void setTime(long time) {
        Instant instant = Instant.ofEpochSecond(time/1000);

        // 将Instant对象转为LocalDateTime对象
        LocalDateTime dateTime = LocalDateTime.ofInstant(instant, ZoneId.of("-3"));
        // 创建日期时间格式化对象
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        this.dayStartTime=dateTime.withHour(0).withMinute(0).withSecond(0).withNano(0).toInstant(ZoneOffset.of("-3")).toEpochMilli();
        this.dateTime=Integer.parseInt(dateTime.format(formatter));
        this.time = time;
    }


    public int getNewUser() {
        int newUser=0;
        if(this.registerTime>=this.dayStartTime&&this.registerTime<this.dayStartTime+86400000){
           newUser=1;
        }
        return newUser;
    }


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
