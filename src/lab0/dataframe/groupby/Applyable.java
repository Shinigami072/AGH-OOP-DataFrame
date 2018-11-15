package lab0.dataframe.groupby;

import lab0.dataframe.DataFrame;
import lab0.dataframe.exceptions.DFApplyableException;

public interface Applyable {

    DataFrame apply(DataFrame df) throws DFApplyableException;

}
