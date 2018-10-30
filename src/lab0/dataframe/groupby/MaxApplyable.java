package lab0.dataframe.groupby;

import lab0.dataframe.DataFrame;
import lab0.dataframe.values.Value;

public class MaxApplyable implements Applyable {

    @Override
    public DataFrame apply(DataFrame df) {
        DataFrame output= new DataFrame(df.getNames(),df.getTypes());

        int size = df.size();
        if(size>0){
            Value[] max = df.getRecord(0);

            for (int i = 1; i < size; i++) {
                Value[] row = df.getRecord(i);

                for (int kolumna = 0; kolumna <max.length ; kolumna++) {
                    try{
                        if(row[kolumna].gte(max[kolumna]))
                            max[kolumna]=row[kolumna];
                    }
                    catch (UnsupportedOperationException ignored){}
                }

            }
            output.addRecord(max);
        }

        return output;
    }
}
