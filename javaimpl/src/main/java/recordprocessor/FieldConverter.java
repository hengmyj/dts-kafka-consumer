package recordprocessor;

import com.alibaba.dts.formats.avro.Field;
import org.apache.commons.lang3.StringUtils;
import recordprocessor.mysql.MysqlFieldConverter;
import recordprocessor.oracle.OracleFieldConverter;
import recordprocessor.postgresql.PostgresqlFieldConverter;

public interface FieldConverter {
    FieldValue convert(Field field, Object o);
    public static FieldConverter getConverter(String sourceName, String sourceVersion) {
        if (StringUtils.endsWithIgnoreCase("mysql", sourceName)) {
            return new MysqlFieldConverter();
        } else if (StringUtils.endsWithIgnoreCase("oracle", sourceName)) {
            return new OracleFieldConverter();
        } else if (StringUtils.endsWithIgnoreCase("postgresql", sourceName)
                || StringUtils.endsWithIgnoreCase("pg", sourceName)) {
            return new PostgresqlFieldConverter();
        }  else {
            throw new RuntimeException("FieldConverter: only mysql supported for now");
        }
    }
}
