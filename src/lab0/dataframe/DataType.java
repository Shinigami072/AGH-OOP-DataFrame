package lab0.dataframe;

import javax.print.DocFlavor;
import java.util.HashMap;
import java.util.Map;

public enum DataType {
    INT("int"),
    FLOAT("float"),
    DOUBLE("double"),
    STRING("string"),
    UNKNOWN("unknown");

    String id;

    DataType(String id){
       this.id=id;
    }

    public static DataType getDataType(String idetification){
        for(DataType dt:DataType.values())
            if(dt.id.equals(idetification))
                return dt;
        return UNKNOWN;
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
