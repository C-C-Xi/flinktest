package ourReduceFuntion;


import entity.LogChangeGold;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;

@Data
@Slf4j
public class LogChangeGoldReduceFuntion1 extends RichReduceFunction<LogChangeGold> {

    private transient ValueState<LogChangeGold> sumState;
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        sumState = getRuntimeContext().getState(new ValueStateDescriptor<>("LogChangeGold", LogChangeGold.class));
    }


    @Override
    public void close() throws Exception {
        super.close();
        // 清理资源等
    }


    @Override
    public LogChangeGold reduce(LogChangeGold logChangeGold, LogChangeGold t1) throws Exception {
        if(sumState.value()==null){
            sumState.update(logChangeGold);
        }else {
            logChangeGold=sumState.value();
        }
        logChangeGold.setChangeNumber(logChangeGold.getChangeNumber()+t1.getChangeNumber());
        logChangeGold.setChangePGoldNumber(logChangeGold.getChangePGoldNumber()+t1.getChangePGoldNumber());
        logChangeGold.setChangeWinningsNumber(logChangeGold.getChangeWinningsNumber()+t1.getChangeWinningsNumber());
        logChangeGold.setChangeWinLock(logChangeGold.getChangeWinLock()+t1.getChangeWinLock());
        logChangeGold.setBalanceNumber(t1.getBalanceNumber());
        logChangeGold.setExt1(logChangeGold.getExt1()+t1.getExt1());
        logChangeGold.setExt2(logChangeGold.getExt2()+t1.getExt2());
        logChangeGold.setExt3(logChangeGold.getExt3()+t1.getExt3());
        logChangeGold.setExt4(logChangeGold.getExt4()+t1.getExt4());
        logChangeGold.setExt5(logChangeGold.getExt5()+t1.getExt5());
        logChangeGold.setExt6(logChangeGold.getExt6()+t1.getExt6());
        logChangeGold.setPureEarn(logChangeGold.getPureEarn()+t1.getPureEarn());
        logChangeGold.setPureCost(logChangeGold.getPureCost()+t1.getPureCost());
        logChangeGold.setRecordCount(logChangeGold.getRecordCount()+1);
        this.sumState.update(logChangeGold);
        return logChangeGold;
    }
}
