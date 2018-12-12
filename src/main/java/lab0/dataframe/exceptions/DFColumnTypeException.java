package lab0.dataframe.exceptions;

import lab0.dataframe.DataFrame;
import lab0.dataframe.values.Value;

public class DFColumnTypeException extends DFColumnException {
    public DFColumnTypeException(DataFrame.Column column, Value o, int id) {
        super(String.format("kolumn:%s value:%s at index: %d", column.getType(), o.getClass(), id), column.getName(), id);
    }
}
