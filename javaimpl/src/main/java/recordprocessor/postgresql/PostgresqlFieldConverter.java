package recordprocessor.postgresql;

import com.alibaba.dts.formats.avro.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import recordprocessor.FieldConverter;
import recordprocessor.FieldValue;

import java.io.IOException;

import static java.nio.charset.StandardCharsets.US_ASCII;

public class PostgresqlFieldConverter implements FieldConverter {
    private static final Logger log = LoggerFactory.getLogger(PostgresqlFieldConverter.class);

    /**
     * PostgreSQL数据类型
     */

    private static final int KRECORD_VERSION = 3;

    //==============================================================================
    //  增量使用的类型列表
    /**
     * data format: String(0101001)
     */
    public static final int BIT = 1;

    /**
     * data format: java.math.BigDecimal.toString()
     */
    public static final int BIG_DECIMAL = 2;

    /**
     * data format: byte[]
     */
    public static final int BYTES = 3;

    /**
     * data format: [0-9]*.[0-9]*
     */
    public static final int FLOAT = 4;

    /**
     * data format: [0-9]*.[0-9]*
     */
    public static final int DOUBLE = 5;

    /**
     * date format: [0-9]*
     * -2^63 (-9223372036854775808) ----> 2^63-1 (9223372036854775807)
     */
    public static final int LONG = 6;

    /**
     * date format: [true|false|t|f]
     */
    public static final int BOOLEAN = 7;

    /**
     * date format: *
     */
    public static final int STRING = 8;

    /**
     * date format: yyyy-MM-dd
     */
    public static final int DATE = 9;

    /**
     * date format: yyyy-MM-dd hh:MM:ss.f
     */
    public static final int DATETIME = 10;

    /**
     * data format: [0-9]*
     */
    public static final int INTEGER = 11;

    /**
     * data format: [0-9]*.f (s)
     */
    public static final int INTERVAL = 12;

    /**
     * data format: hh:MM:ss
     */
    public static final int TIME = 13;

    /**
     * data format: yyyy-MM-dd hh:MM:ss.f
     */
    public static final int TIMESTAMP = 14;

    /**
     * data format: hh:MM:ss.f
     */
    public static final int TIMEMS = 15;

    /**
     * data format: yyyy-MM-dd hh:MM:ss.f [-|+]**:**
     */
    public static final int TIMESTAMPTIMEZONE = 16;

    /**
     * data format: hh:MM:ss [-|+]**:** (with time zone)
     *
     */
    public static final int TIMETZ = 17;

    /**
     * data format: [￥|$]**.**
     */
    public static final int MONEY = 18;

    /**
     * data format: <></>
     */
    public static final int XML = 19;

    /**
     * data format: <(x,y),r>
     */
    public static final int CIRCLE = 20;

    /**
     * data format: [].[].[].[]/[]
     */
    public static final int CIDR = 21;

    /**
     * data format: (x1,y1),(x2,y2)
     */
    public static final int BOX = 22;

    /**
     * data format: [0|1]*
     */
    public static final int VARBIT = 23;

    /**
     * data format: java.util.UUID
     */
    public static final int UUID = 24;

    /**
     * data format: ((x0,y0),(x1,y1),(x2,y2),...,(xn,yn),(x0,y0))
     */
    public static final int POLYGON = 25;

    /**
     * data format: (x,y)
     */
    public static final int POINT = 26;

    /**
     * data format: ((x1,y1),...,(xn,yn))
     */
    public static final int PATH = 27;

    /**
     * data format:[0x]:[0x]:[0x]:[0x]:[0x]:[0x]
     */
    public static final int MACADDR = 28;

    /**
     * data format: [(x1,y1),(x2,y2)]
     */
    public static final int LSEG = 29;

    /**
     * data format: *.*.*.*
     */
    public static final int INET = 30;

    /**
     * data format: yyyy
     */
    public static final int YEAR = 31;

    /**
     * data format: [0-9].*
     */
    public static final int BIGINTEGER = 32;

    /**
     * data format: {x1,x2,x3}
     */
    public static final int LINE = 33;

    /**
     * data format: '[*]*' '[*]*'
     */
    public static final int TSVECTOR = 33;

    /**
     * data format: '[*]*':[*]*,[*]*
     */
    public static final int TSQUERY = 33;

    /**
     * data format: {,,,,}
     */
    public static final int ARRAY = 34;

    /**
     * data fotmat: {...}
     */
    public static final int JSON = 35;

    /**
     * data fotmat: (...)
     */
    public static final int COMPOSITE = 36;

    /**
     * data format: [... , ...)
     */
    public static final int TSRANGE = 37;

    /**
     * data format: k1 => v1, k2 => v2, ...,
     */
    public static final int HSOTRE = 38;

    /**
     * Compatible with all types of Geography
     */
    public static final int GEOMETRY = 39;

    /**
     * data format: [-|+] y-m
     */
    public static final int INTERVALYM = 40;

    /**
     * data format: [-|+] d h:m:s.f
     */
    public static final int INTERVALDS = 41;

    /**
     * data fotmat: [,,,]
     */
    public static final int JSONB = 42;

    /**
     * data format: yyyy-MM-dd hh:MM:ss.f
     */
    public static final int TIMESTAMPLOCALTIMEZONE = 43;
    //  增量使用的类型列表
    //==============================================================================




    //==============================================================================
    // Postgresql11 数据类型

    public static int BOOLOID = 16;
    public static int BYTEAOID = 17;
    public static int CHAROID = 18;
    public static int NAMEOID = 19;
    public static int INT8OID = 20;
    public static int INT2OID = 21;
    public static int INT2VECTOROID = 22;
    public static int INT4OID = 23;
    public static int REGPROCOID = 24;
    public static int TEXTOID = 25;
    public static int OIDOID = 26;
    public static int TIDOID = 27;
    public static int XIDOID = 28;
    public static int CIDOID = 29;
    public static int OIDVECTOROID = 30;
    public static int JSONOID = 114;
    public static int XMLOID = 142;
    public static int XMLARRAYOID = 143;
    public static int JSONARRAYOID = 199;
    public static int PGNODETREEOID = 194;
    public static int PGNDISTINCTOID = 3361;
    public static int PGDEPENDENCIESOID = 3402;
    public static int PGDDLCOMMANDOID = 32;
    public static int SMGROID = 210;
    public static int POINTOID = 600;
    public static int LSEGOID = 601;
    public static int PATHOID = 602;
    public static int BOXOID = 603;
    public static int POLYGONOID = 604;
    public static int LINEOID = 628;
    public static int LINEARRAYOID = 629;
    public static int FLOAT4OID = 700;
    public static int FLOAT8OID = 701;
    public static int ABSTIMEOID = 702;
    public static int RELTIMEOID = 703;
    public static int TINTERVALOID = 704;
    public static int UNKNOWNOID = 705;
    public static int CIRCLEOID = 718;
    public static int CIRCLEARRAYOID = 719;
    public static int CASHOID = 790;
    public static int MONEYARRAYOID = 791;
    public static int MACADDROID = 829;
    public static int INETOID = 869;
    public static int CIDROID = 650;
    public static int MACADDR8OID = 774;
    public static int BOOLARRAYOID = 1000;
    public static int BYTEAARRAYOID = 1001;
    public static int CHARARRAYOID = 1002;
    public static int NAMEARRAYOID = 1003;
    public static int INT2ARRAYOID = 1005;
    public static int INT2VECTORARRAYOID = 1006;
    public static int INT4ARRAYOID = 1007;
    public static int REGPROCARRAYOID = 1008;
    public static int TEXTARRAYOID = 1009;
    public static int OIDARRAYOID = 1028;
    public static int TIDARRAYOID = 1010;
    public static int XIDARRAYOID = 1011;
    public static int CIDARRAYOID = 1012;
    public static int OIDVECTORARRAYOID = 1013;
    public static int BPCHARARRAYOID = 1014;
    public static int VARCHARARRAYOID = 1015;
    public static int INT8ARRAYOID = 1016;
    public static int POINTARRAYOID = 1017;
    public static int LSEGARRAYOID = 1018;
    public static int PATHARRAYOID = 1019;
    public static int BOXARRAYOID = 1020;
    public static int FLOAT4ARRAYOID = 1021;
    public static int FLOAT8ARRAYOID = 1022;
    public static int ABSTIMEARRAYOID = 1023;
    public static int RELTIMEARRAYOID = 1024;
    public static int TINTERVALARRAYOID = 1025;
    public static int POLYGONARRAYOID = 1027;
    public static int ACLITEMOID = 1033;
    public static int ACLITEMARRAYOID = 1034;
    public static int MACADDRARRAYOID = 1040;
    public static int MACADDR8ARRAYOID = 775;
    public static int INETARRAYOID = 1041;
    public static int CIDRARRAYOID = 651;
    public static int CSTRINGARRAYOID = 1263;
    public static int BPCHAROID = 1042;
    public static int VARCHAROID = 1043;
    public static int DATEOID = 1082;
    public static int TIMEOID = 1083;
    public static int TIMESTAMPOID = 1114;
    public static int TIMESTAMPARRAYOID = 1115;
    public static int DATEARRAYOID = 1182;
    public static int TIMEARRAYOID = 1183;
    public static int TIMESTAMPTZOID = 1184;
    public static int TIMESTAMPTZARRAYOID = 1185;
    public static int INTERVALOID = 1186;
    public static int INTERVALARRAYOID = 1187;
    public static int NUMERICARRAYOID = 1231;
    public static int TIMETZOID = 1266;
    public static int TIMETZARRAYOID = 1270;
    public static int BITOID = 1560;
    public static int BITARRAYOID = 1561;
    public static int VARBITOID = 1562;
    public static int VARBITARRAYOID = 1563;
    public static int NUMERICOID = 1700;
    public static int REFCURSOROID = 1790;
    public static int REFCURSORARRAYOID = 2201;
    public static int REGPROCEDUREOID = 2202;
    public static int REGOPEROID = 2203;
    public static int REGOPERATOROID = 2204;
    public static int REGCLASSOID = 2205;
    public static int REGTYPEOID = 2206;
    public static int REGROLEOID = 4096;
    public static int REGNAMESPACEOID = 4089;
    public static int REGPROCEDUREARRAYOID = 2207;
    public static int REGOPERARRAYOID = 2208;
    public static int REGOPERATORARRAYOID = 2209;
    public static int REGCLASSARRAYOID = 2210;
    public static int REGTYPEARRAYOID = 2211;
    public static int REGROLEARRAYOID = 4097;
    public static int REGNAMESPACEARRAYOID = 4090;
    public static int UUIDOID = 2950;
    public static int UUIDARRAYOID = 2951;
    public static int LSNOID = 3220;
    public static int PG_LSNARRAYOID = 3221;
    public static int TSVECTOROID = 3614;
    public static int GTSVECTOROID = 3642;
    public static int TSQUERYOID = 3615;
    public static int REGCONFIGOID = 3734;
    public static int REGDICTIONARYOID = 3769;
    public static int TSVECTORARRAYOID = 3643;
    public static int GTSVECTORARRAYOID = 3644;
    public static int TSQUERYARRAYOID = 3645;
    public static int REGCONFIGARRAYOID = 3735;
    public static int REGDICTIONARYARRAYOID = 3770;
    public static int JSONBOID = 3802;
    public static int JSONBARRAYOID = 3807;
    public static int TXID_SNAPSHOTOID = 2970;
    public static int TXID_SNAPSHOTARRAYOID = 2949;
    public static int INT4RANGEOID = 3904;
    public static int INT4RANGEARRAYOID = 3905;
    public static int NUMRANGEOID = 3906;
    public static int NUMRANGEARRAYOID = 3907;
    public static int TSRANGEOID = 3908;
    public static int TSRANGEARRAYOID = 3909;
    public static int TSTZRANGEOID = 3910;
    public static int TSTZRANGEARRAYOID = 3911;
    public static int DATERANGEOID = 3912;
    public static int DATERANGEARRAYOID = 3913;
    public static int INT8RANGEOID = 3926;
    public static int INT8RANGEARRAYOID = 3927;
    public static int RECORDOID = 2249;
    public static int RECORDARRAYOID = 2287;
    public static int CSTRINGOID = 2275;
    public static int ANYOID = 2276;
    public static int ANYARRAYOID = 2277;
    public static int VOIDOID = 2278;
    public static int TRIGGEROID = 2279;
    public static int EVTTRIGGEROID = 3838;
    public static int LANGUAGE_HANDLEROID = 2280;
    public static int INTERNALOID = 2281;
    public static int OPAQUEOID = 2282;
    public static int ANYELEMENTOID = 2283;
    public static int ANYNONARRAYOID = 2776;
    public static int ANYENUMOID = 3500;
    public static int FDW_HANDLEROID = 3115;
    public static int INDEX_AM_HANDLEROID = 325;
    public static int TSM_HANDLEROID = 3310;
    public static int ANYRANGEOID = 3831;

    // Postgresql11 数据类型
    //==============================================================================
    static DataAdapter[] DATA_ADAPTERS = new DataAdapter[4096];


    @Override
    public FieldValue convert(Field field, Object o) {
        DataAdapter dataAdapter = DATA_ADAPTERS[field.getDataTypeNumber()];

        if(dataAdapter == null) {
            log.error("unknown field data type number: " + field.getDataTypeNumber());
        }

        return dataAdapter.getFieldValue(o);
    }

    static {


        DATA_ADAPTERS[INT2OID] = new NumberStringAdapter(INTEGER);
        DATA_ADAPTERS[INT4OID] = new NumberStringAdapter(INTEGER);
        DATA_ADAPTERS[INT8OID] = new NumberStringAdapter(BIGINTEGER);

        DATA_ADAPTERS[BPCHAROID] = new CharacterAdapter(STRING);
        DATA_ADAPTERS[CHAROID] = new CharacterAdapter(STRING);
        DATA_ADAPTERS[VARCHAROID] = new CharacterAdapter(STRING);
        DATA_ADAPTERS[TEXTOID] = new CharacterAdapter(STRING);

        DATA_ADAPTERS[TIMEOID] = new TimeAdapter(TIME);
        DATA_ADAPTERS[TIMETZOID] = new TimeTimeZoneAdapter(TIMETZ);
        DATA_ADAPTERS[DATEOID] = new DateAdapter(DATE);

        DATA_ADAPTERS[TIMESTAMPOID] = new TimestampStringAdapter(TIMESTAMP);
        DATA_ADAPTERS[TIMESTAMPTZOID] = new TimestampTimeZoneAdapter(TIMESTAMPTIMEZONE);

        DATA_ADAPTERS[BYTEAOID] = new BinaryAdapter(BYTES);

        DATA_ADAPTERS[NUMERICOID] = new DecimalStringAdapter(BIG_DECIMAL);

        DATA_ADAPTERS[FLOAT4OID] = new DoubleStringAdapter(FLOAT);
        DATA_ADAPTERS[FLOAT8OID] = new DoubleStringAdapter(FLOAT);

        DATA_ADAPTERS[BOXOID] = new TextObjectAdapter(BOX);
        DATA_ADAPTERS[CIDROID] = new TextObjectAdapter(CIDR);
        DATA_ADAPTERS[CIRCLEOID] = new TextObjectAdapter(CIRCLE);
        DATA_ADAPTERS[JSONOID] = new TextObjectAdapter(STRING);

        DATA_ADAPTERS[JSONBOID] = new BinaryAdapter(BYTES);

        DATA_ADAPTERS[CASHOID] = new TextObjectAdapter(MONEY);
        DATA_ADAPTERS[LINEOID] = new TextObjectAdapter(LINE);
        DATA_ADAPTERS[LSEGOID] = new TextObjectAdapter(LSEG);
        DATA_ADAPTERS[MACADDROID] = new TextObjectAdapter(MACADDR);
        DATA_ADAPTERS[PATHOID] = new TextObjectAdapter(PATH);
        DATA_ADAPTERS[LSNOID] = new TextObjectAdapter(LONG);
        DATA_ADAPTERS[POINTOID] = new TextObjectAdapter(POINT);
        DATA_ADAPTERS[POLYGONOID] = new TextObjectAdapter(POLYGON);
        DATA_ADAPTERS[INETOID] = new TextObjectAdapter(TIME);
        DATA_ADAPTERS[INTERVALOID] = new TextObjectAdapter(INTERVAL);
        DATA_ADAPTERS[TSQUERYOID] = new TextObjectAdapter(STRING);
        DATA_ADAPTERS[TSVECTOROID] = new TextObjectAdapter(STRING);
        DATA_ADAPTERS[TXID_SNAPSHOTOID] = new TextObjectAdapter(STRING);
        DATA_ADAPTERS[UUIDOID] = new TextObjectAdapter(STRING);
        DATA_ADAPTERS[XMLOID] = new TextObjectAdapter(XML);
        DATA_ADAPTERS[BOOLOID] = new TextObjectAdapter(BOOLEAN);

        DATA_ADAPTERS[BITOID] = new TextObjectAdapter(BIT);
        DATA_ADAPTERS[BITARRAYOID] = new TextObjectAdapter(BIT);
    }


    static interface DataAdapter {

        /**
         *
         * @param data
         * @return 一定不为NULL, BinaryFieldValue.value() 或者FieldValue 可以为NULL
         */
        FieldValue getFieldValue(Object data);

        /**
         * 下游Writer有依赖该类型
         * @return
         */
        int getFieldType();
    }

    static abstract class BaseDataAdapter implements DataAdapter {

        private int xType;
        public BaseDataAdapter(int xType) {
            this.xType = xType;
        }

        @Override
        public int getFieldType() {
            return this.xType;
        }
    }

    static class NumberStringAdapter extends BaseDataAdapter {

        public NumberStringAdapter(int xType) {
            super(xType);
        }

        @Override
        public FieldValue getFieldValue(Object data) {
            FieldValue stringFieldValue = new FieldValue();
            if (null != data) {
                com.alibaba.dts.formats.avro.Integer integer = (com.alibaba.dts.formats.avro.Integer) data;
                stringFieldValue.setValue(integer.getValue().getBytes(US_ASCII));
            }
            return stringFieldValue;
        }
    }

    static class DecimalStringAdapter extends BaseDataAdapter {

        public DecimalStringAdapter(int xType) {
            super(xType);
        }

        public FieldValue getFieldValue(Object data) {
            FieldValue stringFieldValue = new FieldValue();
            if (null != data) {
                com.alibaba.dts.formats.avro.Decimal decimal = (com.alibaba.dts.formats.avro.Decimal)data;
                stringFieldValue.setValue(decimal.getValue().getBytes(US_ASCII));
            }
            return stringFieldValue;
        }
    }

    static class DoubleStringAdapter extends BaseDataAdapter {

        public DoubleStringAdapter(int xType) {
            super(xType);
        }

        public FieldValue getFieldValue(Object data) {
            FieldValue stringFieldValue = new FieldValue();

            if (null != data) {
                com.alibaba.dts.formats.avro.Float aFloat = (com.alibaba.dts.formats.avro.Float) data;
                stringFieldValue.setValue(Double.toString(aFloat.getValue()).getBytes(US_ASCII));
            }
            return stringFieldValue;
        }
    }

    static class CharacterAdapter extends BaseDataAdapter {

        public CharacterAdapter(int xType) {
            super(xType);
        }

        @Override
        public FieldValue getFieldValue(Object data) {
            FieldValue stringFieldValue = new FieldValue();
            if (null != data) {
                com.alibaba.dts.formats.avro.Character character = (com.alibaba.dts.formats.avro.Character) data;

                try {
                    String value = new String(character.getValue().array(), character.getCharset());
                    stringFieldValue.setValue(value.getBytes(US_ASCII));
                } catch (IOException ex) {
                    throw new IllegalArgumentException(ex);
                }
            }
            return stringFieldValue;
        }
    }

    static abstract class AbstractDateTimeAdapter extends BaseDataAdapter {

        static int TIMESTAMP_MILLS_LEN = "0000-00-00 00:00:00.000000".length();
        static int TIMESTAMP_LEN = "0000-00-00 00:00:00".length();
        static int DATE_LEN = "0000-00-00".length();
        static int TIME_LEN = "00:00:00".length();
        static int TIME_MILLS_LEN = "00:00:00.000000".length();

        public AbstractDateTimeAdapter(int xType) {
            super(xType);
        }

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

        void encodeTimeMillis(com.alibaba.dts.formats.avro.DateTime dateTime, byte[] out, int position) {
            if (null != dateTime.getMillis()) {
                int mills = dateTime.getMillis();
                out[position] = '.';
                out[position + 1] = (byte) ('0' + (mills / 100000));
                mills %= 100000;
                out[position + 2] = (byte) ('0' + (mills / 10000));
                mills %= 10000;
                out[position + 3] = (byte) ('0' + (mills / 1000));
                mills %= 1000;
                out[position + 4] = (byte) ('0' + (mills / 100));
                mills %= 100;
                out[position + 5] = (byte) ('0' + (mills / 10));
                out[position + 6] = (byte) ('0' + (mills % 10));
            }
        }
    }

    static class TimeAdapter extends AbstractDateTimeAdapter {

        public TimeAdapter(int xType) {
            super(xType);
        }

        @Override
        public FieldValue getFieldValue(Object data) {
            FieldValue stringFieldValue = new FieldValue();
            if (null != data) {
                com.alibaba.dts.formats.avro.DateTime dateTime = (com.alibaba.dts.formats.avro.DateTime) data;

                byte[] time = null;
                if (null == dateTime.getMillis()) {
                    time = new byte[TIME_LEN];
                } else {
                    time = new byte[TIME_MILLS_LEN];
                }
                encodeTime(dateTime, time, 0);
                encodeTimeMillis(dateTime, time, TIME_LEN);
                stringFieldValue.setValue(new String(time).getBytes(US_ASCII));
            }
            return stringFieldValue;
        }
    }

    static class TimeTimeZoneAdapter extends AbstractDateTimeAdapter {

        public TimeTimeZoneAdapter(int xType) {
            super(xType);
        }

        @Override
        public FieldValue getFieldValue(Object data) {
            FieldValue stringFieldValue = new FieldValue();
            if (null != data) {
                com.alibaba.dts.formats.avro.TimestampWithTimeZone timestampWithTimeZone = (com.alibaba.dts.formats.avro.TimestampWithTimeZone) data;
                com.alibaba.dts.formats.avro.DateTime dateTime = timestampWithTimeZone.getValue();
                byte[] time = null;
                if (null == dateTime.getMillis()) {
                    time = new byte[TIME_LEN];
                } else {
                    time = new byte[TIME_MILLS_LEN];
                }
                encodeTime(dateTime, time, 0);
                encodeTimeMillis(dateTime, time, TIME_LEN);

                StringBuilder timestampTimeZoneBuilder = new StringBuilder();
                timestampTimeZoneBuilder.append(new String(time));
                timestampTimeZoneBuilder.append(timestampWithTimeZone.getTimezone());
                stringFieldValue.setValue(timestampTimeZoneBuilder.toString().getBytes(US_ASCII));
            }
            return stringFieldValue;
        }
    }

    static class TimestampStringAdapter extends AbstractDateTimeAdapter {

        public TimestampStringAdapter(int xType) {
            super(xType);
        }

        public FieldValue getFieldValue(Object data) {

            FieldValue stringFieldValue = new FieldValue();
            if (null != data) {
                com.alibaba.dts.formats.avro.DateTime dateTime = (com.alibaba.dts.formats.avro.DateTime) data;

                byte[] time = null;
                if (null == dateTime.getMillis()) {
                    time = new byte[TIMESTAMP_LEN];
                } else {
                    time = new byte[TIMESTAMP_MILLS_LEN];
                }
                encodeDate(dateTime, time, 0);
                time[10] = ' ';
                encodeTime(dateTime, time, DATE_LEN + 1);
                encodeTimeMillis(dateTime, time, TIMESTAMP_LEN);
                stringFieldValue.setValue(new String(time).getBytes(US_ASCII));
            }
            return stringFieldValue;
        }
    }

    static class TimestampTimeZoneAdapter extends AbstractDateTimeAdapter {

        public TimestampTimeZoneAdapter(int xType) {
            super(xType);
        }

        public FieldValue getFieldValue(Object data) {

            FieldValue stringFieldValue = new FieldValue();
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
                timestampTimeZoneBuilder.append(new String(time));
                timestampTimeZoneBuilder.append(timestampWithTimeZone.getTimezone());
                stringFieldValue.setValue(timestampTimeZoneBuilder.toString().getBytes(US_ASCII));
            }
            return stringFieldValue;
        }
    }

    static class DateAdapter extends AbstractDateTimeAdapter {

        public DateAdapter(int xType) {
            super(xType);
        }

        public FieldValue getFieldValue(Object data) {
            FieldValue stringFieldValue = new FieldValue();
            if (null != data) {
                com.alibaba.dts.formats.avro.DateTime dateTime = (com.alibaba.dts.formats.avro.DateTime) data;

                byte[] time = new byte[DATE_LEN];
                encodeDate(dateTime, time, 0);
                stringFieldValue.setValue(new String(time).getBytes(US_ASCII));
            }
            return stringFieldValue;
        }
    }

    static class BinaryAdapter extends BaseDataAdapter {

        public BinaryAdapter(int xType) {
            super(xType);
        }

        public FieldValue getFieldValue(Object data) {
            FieldValue binaryFieldValue = new FieldValue();
            if (null != data) {
                com.alibaba.dts.formats.avro.BinaryObject binaryObject = (com.alibaba.dts.formats.avro.BinaryObject) data;
                binaryFieldValue.setValue(binaryObject.getValue().array());
            }
            return binaryFieldValue;
        }
    }


    static class TextObjectAdapter extends BaseDataAdapter {

        public TextObjectAdapter(int xType) {
            super(xType);
        }

        public FieldValue getFieldValue(Object data) {

            FieldValue stringFieldValue = new FieldValue();
            if (null != data) {
                if (data instanceof com.alibaba.dts.formats.avro.TextObject) {
                    com.alibaba.dts.formats.avro.TextObject textObject = (com.alibaba.dts.formats.avro.TextObject) data;
                    stringFieldValue.setValue(textObject.getValue().getBytes(US_ASCII));
                } else if (data instanceof com.alibaba.dts.formats.avro.TextGeometry) {
                    com.alibaba.dts.formats.avro.TextGeometry textGeometry = (com.alibaba.dts.formats.avro.TextGeometry) data;
                    stringFieldValue.setValue(textGeometry.getValue().getBytes(US_ASCII));
                } else {
                    throw new RuntimeException("Do not support data type " + data.getClass().getName());
                }
            }
            return stringFieldValue;
        }
    }


}
