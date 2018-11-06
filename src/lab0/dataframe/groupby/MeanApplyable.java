package lab0.dataframe.groupby;

import lab0.dataframe.DataFrame;
import lab0.dataframe.values.DoubleValue;
import lab0.dataframe.values.Value;

public class MeanApplyable implements Applyable{

    @Override
    public DataFrame apply(DataFrame df) {

        DataFrame sum= (new SumApplyable()).apply(df);

        DataFrame output = new DataFrame(sum.getNames(),sum.getTypes());
        //output contains only numeric values
        //if size == 0 it returns an empty Dataframe
        int size = df.size();
        if(size>0){
            Value[] vals = sum.getRecord(0);
            for (int kolumna = 0; kolumna <vals.length ; kolumna++) {
                vals[kolumna]= vals[kolumna].div(new DoubleValue(size));
            }
            output.addRecord(vals);
        }

        return output;
    }


}

