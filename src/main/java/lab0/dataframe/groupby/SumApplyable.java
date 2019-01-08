package lab0.dataframe.groupby;

import lab0.dataframe.DataFrame;
import lab0.dataframe.exceptions.DFApplyableException;
import lab0.dataframe.exceptions.DFColumnTypeException;
import lab0.dataframe.values.NumericValue;
import lab0.dataframe.values.Value;

import java.util.ArrayList;

public class SumApplyable implements Applyable {


    @Override
    public DataFrame apply(DataFrame df) throws DFApplyableException {

        try {
            ArrayList<String> column_names = new ArrayList<>();
            ArrayList<Class<? extends Value>> types = new ArrayList<>();

            Class<? extends Value>[] df_types = df.getTypes();
            String[] df_column_names = df.getNames();
            for (int i = 0; i < df_types.length; i++) {
                if (NumericValue.class.isAssignableFrom(df_types[i])) {
                    column_names.add(df_column_names[i]);
                    types.add(df_types[i]);
                }
            }


            DataFrame output = new DataFrame(column_names.toArray(new String[0]), types.toArray(new Class[0]));
            //https://en.wikipedia.org/wiki/Kahan_summation_algorithm
            //possible accuracy gain
            String[] output_column_names = output.getNames();
            Value[] row = new Value[output.getColCount()];

            int size = df.size();
            if (size > 0) {
                int col = 0;
                for (String colname : output_column_names) {
                    DataFrame.Column k = df.get(colname);
                    row[col] = k.get(0);
                    for (int i = 1; i < size; i++) {
                        row[col] = row[col].add(k.get(i));
                    }
                    col++;
                }
                output.addRecord(row);
            }
            return output;

        } catch (
                DFColumnTypeException e) {
            throw new DFApplyableException(e.getMessage());
        }

    }
}


