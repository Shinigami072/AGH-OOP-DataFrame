package lab0.dataframe.values;

import java.sql.Types;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class TypeEnum {
    public enum Type {

        LONGNVARCHAR(Types.LONGNVARCHAR, StringValue.class),
        LONGVARCHAR(Types.LONGVARCHAR, StringValue.class),
        VARCHAR(Types.VARCHAR, StringValue.class),
        NCHAR(Types.NCHAR, StringValue.class),
        CHAR(Types.CHAR, StringValue.class),
        DATE(Types.DATE, DateTimeValue.class),
        FLOAT(Types.FLOAT, FloatValue.class),
        DECIMAL(Types.DECIMAL, DoubleValue.class),
        DOUBLE(Types.DOUBLE, DoubleValue.class),
        REAL(Types.REAL, DoubleValue.class),
        INTEGER(Types.INTEGER, IntegerValue.class),
        SMALLINT(Types.SMALLINT, IntegerValue.class),
        TINYINT(Types.TINYINT, IntegerValue.class),
        BIGINT(Types.BIGINT, IntegerValue.class),
        ;
        private static final Map<Integer, Class<? extends Value>> SQL_TO_VALUE = Collections.unmodifiableMap(initValues());
        private static final Map<Integer, Type> SQL_TO_TYPE = Collections.unmodifiableMap(initTypes());
        int type;
        Class<? extends Value> value;

        Type(int sqlType, Class<? extends Value> concreteType) {
            this.type = sqlType;
            this.value = concreteType;
        }

        private static HashMap<Integer, Class<? extends Value>> initValues() {
            HashMap<Integer, Class<? extends Value>> map = new HashMap<>(values().length);
            for (Type t : values()) {
                map.put(t.type, t.value);
            }
            return map;
        }

        private static HashMap<Integer, Type> initTypes() {
            HashMap<Integer, Type> map = new HashMap<>(values().length);
            for (Type t : values()) {
                map.put(t.type, t);
            }
            return map;
        }


        public static Type getType(int i) {
            return SQL_TO_TYPE.get(i);
        }

        public static Class<? extends Value> getValueType(int i) {
            return SQL_TO_VALUE.get(i);
        }
    }
}
