package lab0.dataframe.groupby;

import lab0.dataframe.DataFrame;

public interface GroupBy {
//    DataFrame max();
//    DataFrame min();
//
//    DataFrame mean();
//
//    DataFrame std();
//    DataFrame sum();
//    DataFrame var();

    DataFrame apply(Applyable apply);
}
