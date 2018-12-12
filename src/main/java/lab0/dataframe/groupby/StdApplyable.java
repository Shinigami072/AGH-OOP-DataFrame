package lab0.dataframe.groupby;

import lab0.dataframe.DataFrame;
import lab0.dataframe.exceptions.DFApplyableException;
import lab0.dataframe.exceptions.DFColumnTypeException;
import lab0.dataframe.values.DoubleValue;
import lab0.dataframe.values.NumericValue;
import lab0.dataframe.values.Value;

public class StdApplyable implements Applyable {
    @Override
    public DataFrame apply(DataFrame df) throws DFApplyableException {
        try {

            Applyable varianceMaker = new VarApplyable();
            DataFrame var= varianceMaker.apply(df);

            DataFrame output= new DataFrame(var.getNames(),var.getTypes());
            if(df.size()>0){
                DoubleValue sqrt = new DoubleValue(0.5);
                Value[] row = var.getRecord(0);
                for (int i = 0; i < row.length; i++) {
                    row[i]= (row[i].pow(sqrt));
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
