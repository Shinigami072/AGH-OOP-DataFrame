package lab0.dataframe.values;

public abstract class NumericValue extends Value{

    abstract public Number getNumericValue();


    @Override
    public boolean neq(Value v){
        return !eq(v);
    }
}
