package lab0.dataframe.groupby;

import lab0.dataframe.DataFrame;
import lab0.dataframe.values.DoubleValue;
import lab0.dataframe.values.NumericValue;

public class StdApplyable implements Applyable {
    @Override
    public DataFrame apply(DataFrame df) {
        Applyable varianceMaker = new VarApplyable();
        DataFrame var= varianceMaker.apply(df);
        DataFrame output= new DataFrame(var.getNames(),var.getTypes());

        if(df.size()>0){
           NumericValue[] row = (NumericValue[] )var.getRecord(0);
            for (int i = 0; i < row.length; i++) {
                row[i]= new DoubleValue(Math.sqrt(row[i].getValue().doubleValue()));
            }
            output.addRecord(row);
        }

        return output;
    }
}
