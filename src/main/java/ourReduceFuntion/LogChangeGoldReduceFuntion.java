package ourReduceFuntion;


import entity.LogChangeGold;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.ReduceFunction;

@Data
@Slf4j
public class LogChangeGoldReduceFuntion implements ReduceFunction<LogChangeGold> {


    @Override
    public LogChangeGold reduce(LogChangeGold logChangeGold, LogChangeGold t1) throws Exception {
        log.info("logChangeGoldReduceFuntion1--{}", logChangeGold.getTime());
        log.info("t1--{}", t1.getTime());
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
        log.info("result----{}", logChangeGold.getTime());
        return logChangeGold;
    }
}
