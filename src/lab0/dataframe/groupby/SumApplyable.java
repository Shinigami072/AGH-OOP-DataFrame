package lab0.dataframe.groupby;

import lab0.dataframe.DataFrame;
import lab0.dataframe.values.NumericValue;
import lab0.dataframe.values.Value;

import java.util.ArrayList;

public class SumApplyable implements Applyable {



    @Override
    public DataFrame apply(DataFrame df) {

        ArrayList<String> colnames = new ArrayList<>();
        ArrayList<Class<? extends Value>> types = new ArrayList<>();

        Class<? extends Value>[] df_types = df.getTypes();
        String[] df_colnames = df.getNames();
        for (int i=0;i<df_types.length;i++) {
            if(NumericValue.class.isAssignableFrom(df_types[i])){
                colnames.add(df_colnames[i]);
                types.add(df_types[i]);
            }
        }


        DataFrame output = new DataFrame(colnames.toArray(new String[0]),types.toArray(new Class[0]));
        String[] output_colnames = output.getNames();
        Value[] row = new Value[output.colCount()];

        int size = df.size();
        if(size>0) {
            int col = 0;
            for (String colname : output_colnames) {
                DataFrame.Kolumna k = df.get(colname);
                row[col] = k.get(0);
                for (int i = 1; i < size; i++) {
                    row[col] = row[col].add(k.get(i));
                }
                col++;
            }
            output.addRecord(row);
        }

        return output;
    }
}


