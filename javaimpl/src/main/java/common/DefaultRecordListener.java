package common;

import com.alibaba.dts.formats.avro.Operation;

import boot.RecordPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultRecordListener implements RecordListener {
    private static final Logger log = LoggerFactory.getLogger(DefaultRecordListener.class);

    private String dbType;

    private RecordPrinter recordPrinter;

    public DefaultRecordListener(String dbType) {
        this.dbType = dbType;

        recordPrinter = new RecordPrinter(dbType);
    }

    @Override
    public void consume(UserRecord record) {

        Operation operation = record.getRecord().getOperation();

        if(operation.equals(Operation.INSERT)
            || operation.equals(Operation.UPDATE)
            || operation.equals(Operation.DELETE)
            || operation.equals(Operation.DDL)
            || operation.equals(Operation.HEARTBEAT)) {
            // consume record
            String ret = recordPrinter.recordToString(record.getRecord());
            log.info(ret);
        }
        //commit
        log.info("operation : " + operation + ", timestamp: " + record.getRecord().getSourceTimestamp());
        record.commit(String.valueOf(record.getRecord().getSourceTimestamp()));
    }
}
