package lab0.dataframe.exceptions;

import lab0.dataframe.DataFrame;
import lab0.dataframe.SparseDataFrame;
import lab0.dataframe.values.Value;

public class DFColumnTypeException extends DFColumnException {
    public DFColumnTypeException(DataFrame.Kolumna kolumna, Value o, int id) {
        super(String.format("kolumn:%s value:%s at index: %d", kolumna.getType(), o.getClass(), id), kolumna.getNazwa(), id);
    }
}
