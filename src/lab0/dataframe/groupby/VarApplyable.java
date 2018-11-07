package lab0.dataframe.groupby;

import lab0.dataframe.DataFrame;
import lab0.dataframe.values.DoubleValue;
import lab0.dataframe.values.Value;

public class VarApplyable implements Applyable {
    @Override
    public DataFrame apply(DataFrame df) {

        MeanApplyable meanMaker = new MeanApplyable();
        DataFrame mean= meanMaker.apply(df);
        DataFrame output= new DataFrame(mean.getNames(),mean.getTypes());
        DoubleValue sq = new DoubleValue(2.0);
        if(df.size()>0){
            String[] colnames = mean.getNames();
            DataFrame sizedDown = df.get(colnames,false);
            Value[] means = mean.getRecord(0);

            for (int i = 0; i < sizedDown.size(); i++) {
                Value[] row = sizedDown.getRecord(i);
                for (int j = 0; j < means.length; j++) {
                    row[j]=row[j].sub(means[j]);
                    row[j]=row[j].pow(sq);
                }
                output.addRecord(row);
            }
            output=meanMaker.apply(output);
        }

        return output;
    }


}
