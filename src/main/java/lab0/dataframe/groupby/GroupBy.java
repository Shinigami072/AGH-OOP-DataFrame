package lab0.dataframe.groupby;

import lab0.dataframe.DataFrame;
import lab0.dataframe.exceptions.DFApplyableException;
import lab0.dataframe.exceptions.DFColumnTypeException;
import lab0.dataframe.values.Value;

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

    static DataFrame getOutputDataFrame(DataFrame keys, DataFrame group) {
        DataFrame output;
        Class<? extends Value>[] keyTypes = keys.getTypes();
        Class<? extends Value>[] dfTypes = group.getTypes();
        Class<? extends Value>[] fullTypes = new Class[keyTypes.length + dfTypes.length];

        System.arraycopy(keyTypes, 0, fullTypes, 0, keyTypes.length);
        System.arraycopy(dfTypes, 0, fullTypes, keyTypes.length, dfTypes.length);

        String[] keyNames = keys.getNames();
        String[] dfNames = group.getNames();
        String[] fullNames = new String[keyNames.length + dfNames.length];

        System.arraycopy(keyNames, 0, fullNames, 0, keyNames.length);
        System.arraycopy(dfNames, 0, fullNames, keyNames.length, dfNames.length);

        output = new DataFrame(fullNames, fullTypes);
        return output;
    }

    static void addGroup(DataFrame output, Value[] keyValues, DataFrame group) throws DFColumnTypeException {

        Value[] rowValues = new Value[group.getColCount() + keyValues.length];
        System.arraycopy(keyValues, 0, rowValues, 0, keyValues.length);

        for (int j = 0; j < group.size(); j++) {
            Value[] groupValues = group.getRecord(j);

            System.arraycopy(groupValues, 0, rowValues, keyValues.length, groupValues.length);
            output.addRecord(rowValues);

        }
    }
}
