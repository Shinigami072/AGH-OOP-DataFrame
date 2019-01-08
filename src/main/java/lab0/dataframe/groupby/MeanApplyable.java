package lab0.dataframe.groupby;

import lab0.dataframe.DataFrame;
import lab0.dataframe.exceptions.DFApplyableException;
import lab0.dataframe.exceptions.DFColumnTypeException;
import lab0.dataframe.values.DoubleValue;
import lab0.dataframe.values.NumericValue;
import lab0.dataframe.values.Value;

public class MeanApplyable implements Applyable{

    @Override
    public DataFrame apply(DataFrame df) throws DFApplyableException {

        try {
            DataFrame sum= (new SumApplyable()).apply(df);


            Class<DoubleValue>[] types = new Class[sum.getColCount()];
            for (int i = 0; i < sum.getColCount(); i++) {
                types[i]=DoubleValue.class;
            }
            DataFrame output = new DataFrame(sum.getNames(),types);

            //output contains only numeric values
            //if size == 0 it returns an empty DataFrame
            int size = df.size();
            if(size>0){
                DoubleValue[] values = new DoubleValue[output.getColCount()];
                Value[] sums = sum.getRecord(0);
                DoubleValue divider = new DoubleValue(size);
                for (int kolumna = 0; kolumna <values.length ; kolumna++) {
                    values[kolumna]=
                            new DoubleValue(
                                    (((NumericValue)sums[kolumna]).getValue().doubleValue())
                            ).div(divider);
                }
                output.addRecord(values);
            }
            return output;

        } catch (DFColumnTypeException e) {
            throw new DFApplyableException(e.getMessage());
        }

    }


}

