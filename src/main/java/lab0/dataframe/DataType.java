package lab0.dataframe;
@Deprecated
public enum DataType {
    INT("int"),
    FLOAT("float"),
    DOUBLE("double"),
    STRING("string"),
    UNKNOWN("unknown");

    final String id;

    DataType(String id){
       this.id=id;
    }

    public static DataType getDataType(String identification){
        for(DataType dt:DataType.values())
            if(dt.id.equals(identification))
                return dt;
        return UNKNOWN;
    }

    @SuppressWarnings({"CachedNumberConstructorCall", "BoxingBoxedValue", "RedundantStringConstructorCall"})
    public Object cloneData(Object o){//to Traci pamięć niepotrzebnie
        switch(this){
            case INT:
                //noinspection deprecation
                return new Integer(((Integer)o));
            case FLOAT:
                //noinspection deprecation
                return new Float(((Float)o));
            case DOUBLE:
                //noinspection deprecation
                return new Double(((Double)o));
            case STRING:
                return new String((String)o);
            case UNKNOWN:
            default:
                return o;
        }
    }
    public boolean isCorrectType(Object o){
        switch(this){
            case INT:
                return o instanceof Integer;
            case FLOAT:
                return o instanceof Float;
            case DOUBLE:
                return o instanceof Double;
            case STRING:
                return o instanceof String;
            case UNKNOWN:
            default:
                return true;
        }
    }

    public static Object fromString(DataType dt,String data){
        switch(dt){
            case INT:
                return Integer.parseInt(data);
            case FLOAT:
                return Float.parseFloat(data);
            case DOUBLE:
                return Double.parseDouble(data);
            case STRING:
            case UNKNOWN:
                return data;
            default:
                return data;
        }
    }
}
