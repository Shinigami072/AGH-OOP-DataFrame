package lab0.dataframe.groupby;

import lab0.dataframe.DataFrame;

public interface GroupBy {
    default DataFrame max(){
        return apply(new MaxApplyable());
    }
    default DataFrame min(){
        return apply(new MinApplyable());
    }
    default DataFrame mean(){
        return apply(new MeanApplyable());
    }
    default DataFrame std(){
        return apply(new StdApplyable());
    }
    default DataFrame sum(){
        return apply(new SumApplyable());
    }
    default DataFrame var(){
        return apply(new VarApplyable());
    }

    DataFrame apply(Applyable apply);
}
