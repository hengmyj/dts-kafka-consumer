package recordprocessor.oracle;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.alibaba.dts.formats.avro.Field;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import recordprocessor.FieldConverter;
import recordprocessor.FieldValue;

public class OracleFieldConverter implements FieldConverter {
    private static final Logger log = LoggerFactory.getLogger(OracleFieldConverter.class);

    /**
     * Oracle数据类型
     */
    static final int ORACLE_NUMBER = 2;
    static final int ORACLE_BINARY_FLOAT = 100;
    static final int ORACLE_BINARY_DOUBLE = 101;

    static final int ORACLE_VARCHAR2 = 1;
    static final int ORACLE_CHAR = 96;
    static final int ORACLE_CLOB = 112;
    static final int ORACLE_XML = 58;

    static final int ORACLE_DATE = 12;
    static final int ORACLE_TIMESTAMP = 180;
    static final int ORACLE_TIMESTAMP_TZ = 181;
    static final int ORACLE_TIMESTAMP_LTZ = 231;
    static final int ORACLE_INTERVAL_YEAR_TO_MONTH = 182;
    static final int ORACLE_INTERVAL_DAY_TO_SECOND = 183;

    static final int ORACLE_ROWID = 69;
    static final int ORACLE_UROWID = 208;
    static final int ORACLE_BLOB = 113;

    static final int ORACLE_LONG = 8;
    static final int ORACLE_RAW = 23;
    static final int ORACLE_LONG_RAW = 24;

    static final int ORACLE_BFILE = 114;

    static DataAdapter[] DATA_ADAPTERS = new DataAdapter[256];

    @Override
    public FieldValue convert(Field field, Object o) {
         DataAdapter dataAdapter = DATA_ADAPTERS[field.getDataTypeNumber()];

         if(dataAdapter == null) {
             log.error("unknown field data type number: " + field.getDataTypeNumber());
         }

        return dataAdapter.getFieldValue(o);
    }

    static {

        DATA_ADAPTERS[ORACLE_NUMBER] = new DecimalStringAdapter();
        DATA_ADAPTERS[ORACLE_BINARY_FLOAT] = new DoubleStringAdapter();
        DATA_ADAPTERS[ORACLE_BINARY_DOUBLE] = new DoubleStringAdapter();

        DATA_ADAPTERS[ORACLE_CHAR] = new CharacterAdapter();
        DATA_ADAPTERS[ORACLE_VARCHAR2] = new CharacterAdapter();
        DATA_ADAPTERS[ORACLE_CLOB] = new CharacterAdapter();
        DATA_ADAPTERS[ORACLE_LONG] = new CharacterAdapter();
        DATA_ADAPTERS[ORACLE_XML] = new CharacterAdapter();

        DATA_ADAPTERS[ORACLE_DATE] = new DateAdapter();
        DATA_ADAPTERS[ORACLE_TIMESTAMP] = new TimestampStringAdapter();
        DATA_ADAPTERS[ORACLE_TIMESTAMP_LTZ] = new TimestampTimeZoneAdapter();
        DATA_ADAPTERS[ORACLE_TIMESTAMP_TZ] = new TimestampTimeZoneAdapter();

        DATA_ADAPTERS[ORACLE_ROWID] = new TextObjectAdapter();
        DATA_ADAPTERS[ORACLE_UROWID] = new TextObjectAdapter();
        DATA_ADAPTERS[ORACLE_BLOB] = new BinaryAdapter();
        DATA_ADAPTERS[ORACLE_RAW] = new BinaryAdapter();
        DATA_ADAPTERS[ORACLE_LONG_RAW] = new BinaryAdapter();

        DATA_ADAPTERS[ORACLE_BFILE] = new TextObjectAdapter();

        DATA_ADAPTERS[ORACLE_INTERVAL_YEAR_TO_MONTH] = new IntervalYearToMonthAdapter();
        DATA_ADAPTERS[ORACLE_INTERVAL_DAY_TO_SECOND] = new IntervalDayToSecondAdapter();
    }

    interface DataAdapter {

        /**
         * @param data
         * @return 一定不为NULL FieldValue 可以为NULL
         */
        FieldValue getFieldValue(Object data);

        int getRawType();

    }


    static class DecimalStringAdapter implements DataAdapter {

        static final int BIG_DECIMAL = 2; //XTypes.BIG_DECIMAL

        public FieldValue getFieldValue(Object data) {
            FieldValue fieldValue = new FieldValue();
            if (null != data) {
                com.alibaba.dts.formats.avro.Decimal decimal = (com.alibaba.dts.formats.avro.Decimal) data;
                fieldValue.setValue(decimal.getValue().getBytes(US_ASCII));
            }
            fieldValue.setEncoding("ASCII");

            return fieldValue;
        }

        @Override
        public int getRawType() {
            return BIG_DECIMAL;
        }
    }

    static class DoubleStringAdapter implements DataAdapter {

        static final int DOUBLE = 5;

        public FieldValue getFieldValue(Object data) {
            FieldValue FieldValue = new FieldValue();

            if (null != data) {
                com.alibaba.dts.formats.avro.Float aFloat = (com.alibaba.dts.formats.avro.Float) data;
                FieldValue.setValue(Double.toString(aFloat.getValue()).getBytes(US_ASCII));
            }
            return FieldValue;
        }

        @Override
        public int getRawType() {
            return DOUBLE;
        }
    }

    static abstract class AbstractDateTimeAdapter implements DataAdapter {

        static int TIMESTAMP_MILLS_LEN = "0000-00-00 00:00:00.000000000".length();
        static int TIMESTAMP_LEN = "0000-00-00 00:00:00".length();
        static int DATE_LEN = "0000-00-00".length();

        void encodeDate(com.alibaba.dts.formats.avro.DateTime dateTime, byte[] out, int position) {
            if (null != dateTime && null != out) {
                out[position] = (byte) ('0' + (dateTime.getYear() / 1000));
                out[position + 1] = (byte) ('0' + (dateTime.getYear() % 1000 / 100));
                out[position + 2] = (byte) ('0' + (dateTime.getYear() % 100 / 10));
                out[position + 3] = (byte) ('0' + (dateTime.getYear() % 10));
                out[position + 4] = '-';
                out[position + 5] = (byte) ('0' + (dateTime.getMonth() / 10));
                out[position + 6] = (byte) ('0' + (dateTime.getMonth() % 10));
                out[position + 7] = '-';
                out[position + 8] = (byte) ('0' + (dateTime.getDay() / 10));
                out[position + 9] = (byte) ('0' + (dateTime.getDay() % 10));
            }
        }

        void encodeTime(com.alibaba.dts.formats.avro.DateTime dateTime, byte[] out, int position) {
            if (null != dateTime && null != out) {
                out[position + 0] = (byte) ('0' + (dateTime.getHour() / 10));
                out[position + 1] = (byte) ('0' + (dateTime.getHour() % 10));
                out[position + 2] = ':';
                out[position + 3] = (byte) ('0' + (dateTime.getMinute() / 10));
                out[position + 4] = (byte) ('0' + (dateTime.getMinute() % 10));
                out[position + 5] = ':';
                out[position + 6] = (byte) ('0' + (dateTime.getSecond() / 10));
                out[position + 7] = (byte) ('0' + (dateTime.getSecond() % 10));
            }
        }

        /**
         * Oracle可以精确到纳秒
         *
         * @param dateTime
         * @param out
         * @param position
         */
        void encodeTimeMillis(com.alibaba.dts.formats.avro.DateTime dateTime, byte[] out, int position) {
            if (null != dateTime.getMillis()) {
                int mills = dateTime.getMillis();
                out[position] = '.';
                out[position + 1] = (byte) ('0' + (mills / 100000000));
                mills %= 100000000;
                out[position + 2] = (byte) ('0' + (mills / 10000000));
                mills %= 10000000;
                out[position + 3] = (byte) ('0' + (mills / 1000000));
                mills %= 1000000;
                out[position + 4] = (byte) ('0' + (mills / 100000));
                mills %= 100000;
                out[position + 5] = (byte) ('0' + (mills / 10000));
                mills %= 10000;
                out[position + 6] = (byte) ('0' + (mills / 1000));
                mills %= 1000;
                out[position + 7] = (byte) ('0' + (mills / 100));
                mills %= 100;
                out[position + 8] = (byte) ('0' + (mills / 10));
                out[position + 9] = (byte) ('0' + (mills % 10));
            }
        }
    }

    static class TimestampStringAdapter extends AbstractDateTimeAdapter {

        static final int TIMESTAMP = 14;

        protected String dateTimeToString(com.alibaba.dts.formats.avro.DateTime dateTime) {
            byte[] time = null;
            if (null != dateTime.getMillis()) {
                time = new byte[TIMESTAMP_MILLS_LEN];
            } else {
                time = new byte[TIMESTAMP_LEN];
            }
            encodeDate(dateTime, time, 0);
            time[10] = ' ';
            encodeTime(dateTime, time, DATE_LEN + 1);
            encodeTimeMillis(dateTime, time, TIMESTAMP_LEN);
            return new String(time);
        }

        public FieldValue getFieldValue(Object data) {

            FieldValue fieldValue = new FieldValue();
            if (null != data) {
                com.alibaba.dts.formats.avro.DateTime dateTime = (com.alibaba.dts.formats.avro.DateTime) data;
                fieldValue.setValue(dateTimeToString(dateTime).getBytes(US_ASCII));
            }
            fieldValue.setEncoding("ASCII");

            return fieldValue;
        }

        @Override
        public int getRawType() {
            return TIMESTAMP;
        }
    }

    static class TimestampTimeZoneAdapter extends AbstractDateTimeAdapter {

        static final int TIMESTAMPTIMEZONE = 16;

        public FieldValue getFieldValue(Object data) {

            FieldValue fieldValue = new FieldValue();
            if (null != data) {
                com.alibaba.dts.formats.avro.TimestampWithTimeZone timestampWithTimeZone = (com.alibaba.dts.formats.avro.TimestampWithTimeZone) data;
                com.alibaba.dts.formats.avro.DateTime dateTime = timestampWithTimeZone.getValue();
                byte[] time = null;
                if (null != dateTime.getMillis()) {
                    time = new byte[TIMESTAMP_MILLS_LEN];
                } else {
                    time = new byte[TIMESTAMP_LEN];
                }
                encodeDate(dateTime, time, 0);
                time[10] = ' ';
                encodeTime(dateTime, time, DATE_LEN + 1);
                encodeTimeMillis(dateTime, time, TIMESTAMP_LEN);

                StringBuilder timestampTimeZoneBuilder = new StringBuilder();
                timestampTimeZoneBuilder.append(new String(time)).append(' ');
                timestampTimeZoneBuilder.append(timestampWithTimeZone.getTimezone());
                fieldValue.setValue(timestampTimeZoneBuilder.toString().getBytes(US_ASCII));
            }

            fieldValue.setEncoding("ASCII");

            return fieldValue;
        }

        @Override
        public int getRawType() {
            return TIMESTAMPTIMEZONE;
        }
    }

    static class TimestampLocalTimeZoneAdapter extends TimestampStringAdapter {

        public FieldValue getFieldValue(Object data) {

            FieldValue fieldValue = new FieldValue();
            if (null != data) {
                com.alibaba.dts.formats.avro.DateTime dateTime = ((com.alibaba.dts.formats.avro.TimestampWithTimeZone) data).getValue();
                fieldValue.setValue(dateTimeToString(dateTime).getBytes(US_ASCII));
            }

            fieldValue.setEncoding("ASCII");

            return fieldValue;
        }
    }

    static class DateAdapter extends AbstractDateTimeAdapter {

        static final int DATETIME = 10;

        public FieldValue getFieldValue(Object data) {
            FieldValue fieldValue = new FieldValue();
            if (null != data) {
                com.alibaba.dts.formats.avro.DateTime dateTime = (com.alibaba.dts.formats.avro.DateTime) data;
                byte[] time = null;
                int position = 0;
                if (dateTime.getYear() < 0) {
                    dateTime.setYear(-dateTime.getYear());
                    time = new byte[TIMESTAMP_LEN + 1];
                    time[0] = '-';
                    position = 1;
                } else {
                    time = new byte[TIMESTAMP_LEN];
                }
                encodeDate(dateTime, time, position);
                time[position + 10] = ' ';
                encodeTime(dateTime, time, position + DATE_LEN + 1);
                fieldValue.setValue(time);
            }
            fieldValue.setEncoding("ASCII");
            return fieldValue;
        }

        @Override
        public int getRawType() {
            return DATETIME;
        }
    }

    static class BinaryAdapter implements DataAdapter {
        static final int BYTES = 3;

        public FieldValue getFieldValue(Object data) {

            FieldValue fieldValue = new FieldValue();
            if (null != data) {
                com.alibaba.dts.formats.avro.BinaryObject binaryObject = (com.alibaba.dts.formats.avro.BinaryObject) data;
                fieldValue.setValue(binaryObject.getValue().array());
            }
            return fieldValue;
        }

        @Override
        public int getRawType() {
            return BYTES;
        }
    }


    static class TextObjectAdapter implements DataAdapter {

        static final int STRING = 8;

        public FieldValue getFieldValue(Object data) {

            FieldValue FieldValue = new FieldValue();
            if (null != data) {

                com.alibaba.dts.formats.avro.TextObject textObject = (com.alibaba.dts.formats.avro.TextObject) data;
                FieldValue.setValue(textObject.getValue().getBytes(US_ASCII));
            }
            return FieldValue;
        }

        @Override
        public int getRawType() {
            return STRING;
        }
    }

    static class CharacterAdapter implements DataAdapter {
        static final int STRING = 8; //XTypes.STRING


        public FieldValue getFieldValue(Object data) {

            FieldValue fieldValue = new FieldValue();
            if (null != data) {
                com.alibaba.dts.formats.avro.Character character = (com.alibaba.dts.formats.avro.Character) data;
                fieldValue.setValue(character.getValue().array());
                fieldValue.setEncoding(character.getCharset());
            } else {
                fieldValue.setEncoding("ASCII");
            }
            return fieldValue;
        }
        @Override
        public int getRawType() {
            return STRING;
        }
    }


    static class IntervalYearToMonthAdapter implements DataAdapter {

        static final int STRING = 8; //XTypes.STRING

        public FieldValue getFieldValue(Object data) {

            FieldValue FieldValue = new FieldValue();
            if (null != data) {
                StringBuilder builder = new StringBuilder();
                com.alibaba.dts.formats.avro.DateTime dateTime = (com.alibaba.dts.formats.avro.DateTime) data;
                boolean neg = (dateTime.getYear() < 0);
                if (neg) {
                    builder.append('-');
                    builder.append(-dateTime.getYear());
                    builder.append('-');
                    builder.append(-dateTime.getMonth());
                } else {
                    builder.append(dateTime.getYear());
                    builder.append('-');
                    builder.append(dateTime.getMonth());
                }
                FieldValue.setValue(builder.toString().getBytes(US_ASCII));
            }
            return FieldValue;
        }

        @Override
        public int getRawType() {
            return STRING;
        }
    }


    static class IntervalDayToSecondAdapter implements DataAdapter {

        static final int STRING = 8; //XTypes.STRING


        public String formatOracleNanos(int mills) {
            byte[] out = new byte[9];
            out[0] = (byte) ('0' + (mills / 100000000));
            mills %= 100000000;
            out[1] = (byte) ('0' + (mills / 10000000));
            mills %= 10000000;
            out[2] = (byte) ('0' + (mills / 1000000));
            mills %= 1000000;
            out[3] = (byte) ('0' + (mills / 100000));
            mills %= 100000;
            out[4] = (byte) ('0' + (mills / 10000));
            mills %= 10000;
            out[5] = (byte) ('0' + (mills / 1000));
            mills %= 1000;
            out[6] = (byte) ('0' + (mills / 100));
            mills %= 100;
            out[7] = (byte) ('0' + (mills / 10));
            out[8] = (byte) ('0' + (mills % 10));
            return new String(out);
        }


        public FieldValue getFieldValue(Object data) {

            FieldValue FieldValue = new FieldValue();
            if (null != data) {
                StringBuilder builder = new StringBuilder();
                com.alibaba.dts.formats.avro.DateTime dateTime = (com.alibaba.dts.formats.avro.DateTime) data;
                boolean neg = (dateTime.getDay() < 0);
                if (neg) {
                    builder.append('-');
                    builder.append(-dateTime.getDay());
                    builder.append(' ');
                    builder.append(-dateTime.getHour());
                    builder.append(':');
                    builder.append(-dateTime.getMinute());
                    builder.append(':');
                    builder.append(-dateTime.getSecond());
                    if (null != dateTime.getMillis()) {
                        builder.append('.').append(formatOracleNanos(-dateTime.getMillis()));
                    }
                } else {
                    builder.append(dateTime.getDay());
                    builder.append(' ');
                    builder.append(dateTime.getHour());
                    builder.append(':');
                    builder.append(dateTime.getMinute());
                    builder.append(':');
                    builder.append(dateTime.getSecond());
                    if (null != dateTime.getMillis()) {
                        builder.append('.').append(formatOracleNanos(dateTime.getMillis()));
                    }
                }
                FieldValue.setValue(builder.toString().getBytes(US_ASCII));
            }
            return FieldValue;
        }

        @Override
        public int getRawType() {
            return STRING;
        }
    }
}
