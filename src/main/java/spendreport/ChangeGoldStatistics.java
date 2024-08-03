package spendreport;

import entity.LogChangeGold;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;

@Data
public class ChangeGoldStatistics extends KeyedProcessFunction<Long, LogChangeGold, Alert> {
    private static final long serialVersionUID = 1L;

    private static final double SMALL_AMOUNT = 1.00;
    private static final double LARGE_AMOUNT = 500.00;
    private static final long ONE_MINUTE = 60 * 1000;

    @Override
    public void processElement(LogChangeGold logChangeGold, KeyedProcessFunction<Long, LogChangeGold, Alert>.Context context, Collector<Alert> collector) throws Exception {
        Alert alert = new Alert();
        if(logChangeGold.getAppType()==155277){
            alert.setId(logChangeGold.getPlayerId());

            collector.collect(alert);
        }
    }
//    @Override
//    public void processElement(LogChangeGold logChangeGold, KeyedProcessFunction<Integer,
//            LogChangeGold, Alert>.Context context, Collector<Alert> collector) throws Exception {
//
//
//    }
}
