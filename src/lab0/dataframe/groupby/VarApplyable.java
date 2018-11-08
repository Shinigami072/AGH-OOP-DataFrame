package lab0.dataframe.groupby;

import lab0.dataframe.DataFrame;
import lab0.dataframe.values.DoubleValue;
import lab0.dataframe.values.Value;

public class VarApplyable implements Applyable {
    @Override
    public DataFrame apply(DataFrame df) {

        MeanApplyable meanMaker = new MeanApplyable();
        SumApplyable sumMaker = new SumApplyable();
        DataFrame mean= meanMaker.apply(df);
        DataFrame output= new DataFrame(mean.getNames(),mean.getTypes());
        DoubleValue sq = new DoubleValue(2.0);
        //problemy numeryczne - https://www.johndcook.com/blog/standard_deviation/ możliwe rozwiązanie
        //https://en.wikipedia.org/wiki/Kahan_summation_algorithm
        //possible aqquarcy gain
        if(df.size()>0){
            String[] colnames = mean.getNames();
            DataFrame sizedDown = df.get(colnames,false);
            Value[] means = mean.getRecord(0);
            Value[] variance = new Value[mean.colCount()];
            for (int i = 0; i < sizedDown.size(); i++) {
                Value[] row = sizedDown.getRecord(i);
                for (int j = 0; j < means.length; j++) {
                    if(variance[j] ==null)
                        variance[j]=row[j].sub(means[j]).pow(sq);
                    else
                        variance[j]=variance[j].add(row[j].sub(means[j]).pow(sq));
                }
            }
            DoubleValue divider = new DoubleValue(sizedDown.size()-1);
            for (int j = 0; j < means.length; j++) {
                variance[j]=variance[j].div(divider);
            }
            output.addRecord(variance);
        }

        return output;
    }


}
