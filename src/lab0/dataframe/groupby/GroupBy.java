package lab0.dataframe.groupby;

import lab0.dataframe.DataFrame;
import lab0.dataframe.exceptions.DFApplyableException;

public interface GroupBy {
    default DataFrame max() throws DFApplyableException {
        return apply(new MaxApplyable());
    }

    default DataFrame min() throws DFApplyableException {
        return apply(new MinApplyable());
    }

    default DataFrame mean() throws DFApplyableException {
        return apply(new MeanApplyable());
    }

    default DataFrame std() throws DFApplyableException {
        return apply(new StdApplyable());
    }

    default DataFrame sum() throws DFApplyableException {
        return apply(new SumApplyable());
    }

    default DataFrame var() throws DFApplyableException {
        return apply(new VarApplyable());
    }

    DataFrame apply(Applyable apply) throws DFApplyableException;
}
