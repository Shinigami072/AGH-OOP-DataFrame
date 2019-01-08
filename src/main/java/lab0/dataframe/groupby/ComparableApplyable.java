package lab0.dataframe.groupby;

import lab0.dataframe.DataFrame;
import lab0.dataframe.exceptions.DFApplyableException;
import lab0.dataframe.exceptions.DFColumnTypeException;
import lab0.dataframe.values.Value;

import java.util.HashSet;
import java.util.Set;

public abstract class ComparableApplyable implements Applyable {

    protected abstract void comparison(Value[] optimal, Value[] tested, Set<Integer> banned);

    protected String[] restNames(String[] output_column_names, Set<Integer> banned) {
        String[] column_names = new String[output_column_names.length - banned.size()];

        for (int i = 0, j = 0; i < output_column_names.length; i++) {
            if (!banned.contains(i))
                column_names[j++] = output_column_names[i];
        }
        return column_names;
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
                throw new DFApplyableException("no comparable Columns in Data frame");

            String[] column_names = restNames(output.getNames(), bannedColumns);

            return output.get(column_names, false);

        } catch (DFColumnTypeException | CloneNotSupportedException e) {
            throw new DFApplyableException(e.getMessage());//todo: Applyable Type error
        }
    }
}
