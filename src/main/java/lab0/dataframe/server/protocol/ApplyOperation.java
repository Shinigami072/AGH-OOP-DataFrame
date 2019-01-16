package lab0.dataframe.server.protocol;

import lab0.dataframe.groupby.*;

public enum ApplyOperation {
    MIN(new MinApplyable()),
    MAX(new MaxApplyable()),
    MEAN(new MeanApplyable()),
    STD(new StdApplyable()),
    VAR(new VarApplyable()),
    SUM(new SumApplyable());
    private Applyable applyable;

    ApplyOperation(Applyable applyable) {
        this.applyable=applyable;
    }

    public Applyable getApplyable() {
        return applyable;
    }

    public void setApplyable(Applyable applyable) {
        this.applyable = applyable;
    }
}
