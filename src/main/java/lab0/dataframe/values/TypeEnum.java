package lab0.dataframe.values;

import java.sql.Types;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class TypeEnum {
    public enum Type {

        LONG_NVARCHAR(Types.LONGNVARCHAR, StringValue.class, "nvarchar"),
        LONG_VARCHAR(Types.LONGVARCHAR, StringValue.class, "nvarchar"),
        NCHAR(Types.NCHAR, StringValue.class, "char"),
        CHAR(Types.CHAR, StringValue.class, "char"),
        VARCHAR(Types.VARCHAR, StringValue.class, "varchar(255)"),
        DATE(Types.DATE, DateTimeValue.class, "date"),
        FLOAT(Types.FLOAT, FloatValue.class, "float(24,7)"),
        DECIMAL(Types.DECIMAL, DoubleValue.class, "decimal"),
        REAL(Types.REAL, DoubleValue.class, "real"),
        DOUBLE(Types.DOUBLE, DoubleValue.class, "float(53,7)"),
        SMALLINT(Types.SMALLINT, IntegerValue.class, "smallint"),
        TINYINT(Types.TINYINT, IntegerValue.class, "tinyint"),
        BIGINT(Types.BIGINT, IntegerValue.class, "bigint"),
        INTEGER(Types.INTEGER, IntegerValue.class, "int");
        private static final Map<Integer, Class<? extends Value>> SQL_TO_VALUE = Collections.unmodifiableMap(initValues());
        private static final Map<Integer, Type> SQL_TO_TYPE = Collections.unmodifiableMap(initTypes());
        private static final Map<Class<? extends Value>, Type> CLASS_TO_TYPE = Collections.unmodifiableMap(initClass());

        private static Map<Class<? extends Value>, Type> initClass() {
            HashMap<Class<? extends Value>, Type> map = new HashMap<>(values().length);
            for (Type t : values()) {
                map.put(t.value, t);
            }
            return map;
        }

        final int type;
        final Class<? extends Value> value;
        final String sql;

        Type(int sqlType, Class<? extends Value> concreteType, String sql) {
            this.type = sqlType;
            this.value = concreteType;
            this.sql = sql;
        }

        public String getSql() {

            return sql;
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

        public static Type getType(Class<? extends Value> t) {
            return CLASS_TO_TYPE.get(t);
        }

        public static Type getType(int i) {
            return SQL_TO_TYPE.get(i);
        }

        public static Class<? extends Value> getValueType(int i) {
            return SQL_TO_VALUE.get(i);
        }
    }
}
