package lab0.dataframe.groupby;

import lab0.dataframe.DataFrame;
import lab0.dataframe.values.DoubleValue;
import lab0.dataframe.values.NumericValue;
import lab0.dataframe.values.Value;

public class MeanApplyable implements Applyable{

    @Override
    public DataFrame apply(DataFrame df) {
        DataFrame output= new DataFrame(df.getNames(),df.getTypes());

        int size = df.size();
        if(size>0){
            Value[] vals = df.getRecord(0);
            String[] names = df.getNames();

            for (int kolumna = 0; kolumna <vals.length ; kolumna++) {
                DataFrame.Kolumna k = df.get(names[kolumna]);

                try{
                if(NumericValue.class.isAssignableFrom(k.getType())){

                    for (int i = 1; i < size; i++) {
                        Value row = k.get(i);
                        vals[kolumna]= vals[kolumna].add(row);
                    }

                    vals[kolumna]= vals[kolumna].div(new DoubleValue(size));
                }
                }catch (UnsupportedOperationException ignored){ }

            }

            output.addRecord(vals);
        }

        return output;
    }


}

