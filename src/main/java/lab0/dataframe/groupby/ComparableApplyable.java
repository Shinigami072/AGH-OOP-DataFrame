package lab0.dataframe.groupby;

import lab0.dataframe.DataFrame;
import lab0.dataframe.exceptions.DFApplyableException;
import lab0.dataframe.exceptions.DFColumnTypeException;
import lab0.dataframe.values.Value;

import java.util.HashSet;
import java.util.Set;

public abstract class ComparableApplyable implements Applyable {

    protected abstract void comparison(Value[] optimal, Value[] tested, Set<Integer> banned);

    protected String[] restNames(String[] output_colnames, Set<Integer> banned) {
        String[] colnames = new String[output_colnames.length - banned.size()];

        for (int i = 0, j = 0; i < output_colnames.length; i++) {
            if (!banned.contains(i))
                colnames[j++] = output_colnames[i];
        }
        return colnames;
    }

    @Override
    public DataFrame apply(DataFrame df) throws DFApplyableException {
        DataFrame output = new DataFrame(df.getNames(), df.getTypes());

        HashSet<Integer> bannedColumns = new HashSet<>();
        try {
            int size = df.size();
            if (size > 0) {
                Value[] optimal = df.getRecord(0);

                for (int i = 1; i < size; i++) {
                    Value[] row = df.getRecord(i);
                    comparison(optimal, row, bannedColumns);

                }
                output.addRecord(optimal);
            }

            if (bannedColumns.size() == output.getColCount())
                throw new DFApplyableException("no comparable Columns in Dataframe");

            String[] colnames = restNames(output.getNames(), bannedColumns);

            return output.get(colnames, false);

        } catch (DFColumnTypeException | CloneNotSupportedException e) {
            throw new DFApplyableException(e.getMessage());//todo: Applyable Type error
        }
    }
}
