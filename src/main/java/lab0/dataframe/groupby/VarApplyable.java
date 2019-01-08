package lab0.dataframe.groupby;

import lab0.dataframe.DataFrame;
import lab0.dataframe.exceptions.DFApplyableException;
import lab0.dataframe.exceptions.DFColumnTypeException;
import lab0.dataframe.values.DoubleValue;
import lab0.dataframe.values.NumericValue;
import lab0.dataframe.values.Value;

public class VarApplyable implements Applyable {
    @Override
    public DataFrame apply(DataFrame df) throws DFApplyableException {
        try {

            MeanApplyable meanMaker = new MeanApplyable();
            DataFrame mean = meanMaker.apply(df);
            DataFrame output = new DataFrame(mean.getNames(), mean.getTypes());
            //problemy numeryczne - https://www.johndcook.com/blog/standard_deviation/ możliwe rozwiązanie
            //https://en.wikipedia.org/wiki/Kahan_summation_algorithm
            //possible accuracy gain
            if (df.size() > 1) {

                Value[] means = mean.getRecord(0);

                String[] column_names = mean.getNames();
                DataFrame sizedDown = df.get(column_names, false);

                DoubleValue sq = new DoubleValue(2.0);

                Value[] variance = new Value[mean.getColCount()];

                for (int i = 0; i < sizedDown.size(); i++) {
                    Value[] data = sizedDown.getRecord(i);
                    for (int j = 0; j < means.length; j++) {
                        //wartość w double
                        DoubleValue val = new DoubleValue(((NumericValue) data[j]).getValue().doubleValue());


                        if (variance[j] == null)
                            variance[j] = val.sub(means[j]).pow(sq);//kwadraty różnic od średniej
                        else
                            variance[j] = variance[j].add(val.sub(means[j]).pow(sq));
                    }
                }

                //podzielenie sumy kwadratów przez n-1
                DoubleValue divider = new DoubleValue(sizedDown.size() - 1);
                for (int j = 0; j < means.length; j++) {
                    variance[j] = variance[j].div(divider);
                }

                //dodanie wyników
                output.addRecord(variance);
                return output;

            } else if (df.size() == 1) {

                Value[] variance = new Value[mean.getColCount()];
                for (int i = 0; i < variance.length; i++) {
                    variance[i] = new DoubleValue(0.0);
                }
                output.addRecord(variance);
                return output;

            } else {
                return output;
            }

        } catch (
                DFColumnTypeException | CloneNotSupportedException e) {
            throw new DFApplyableException(e.getMessage());
        }

    }


}
