package lab0.dataframe.values;

public class IntegerValue extends NumericValue {

    Integer value;

    @Override
    public Number getNumericValue() {
        return value;
    }

    public IntegerValue(int i){
        value=i;
    }

    @Override
    public Integer getValue() {
        return value;
    }

    @Override
    public String toString() {
        return value.toString();
    }

    @Override
    public IntegerValue create(String s) {
        return new IntegerValue(Integer.parseInt(s));
    }

    @Override
    public IntegerValue add(Value v) throws UnsupportedOperationException{
        if(! (v instanceof NumericValue))
            throw new UnsupportedOperationException();

        return new IntegerValue(value+((NumericValue)v).getNumericValue().intValue());
    }

    @Override
    public IntegerValue sub(Value v) throws UnsupportedOperationException{
        if(! (v instanceof NumericValue))
            throw new UnsupportedOperationException();

        return new IntegerValue(value-((NumericValue)v).getNumericValue().intValue());
    }

    @Override
    public IntegerValue mul(Value v) throws UnsupportedOperationException{
        if(! (v instanceof NumericValue))
            throw new UnsupportedOperationException();

        return new IntegerValue(value*((NumericValue)v).getNumericValue().intValue());
    }

    @Override
    public IntegerValue div(Value v) throws UnsupportedOperationException{
        if(! (v instanceof NumericValue))
            throw new UnsupportedOperationException();

        return new IntegerValue(value/((NumericValue)v).getNumericValue().intValue());
    }

    @Override
    public DoubleValue pow(Value v) throws UnsupportedOperationException{
        if(! (v instanceof NumericValue))
            throw new UnsupportedOperationException();

        return new DoubleValue(Math.pow(value,((NumericValue)v).getNumericValue().doubleValue()));    }

    @Override
    public boolean eq(Value v) throws UnsupportedOperationException{
        if(! (v instanceof NumericValue))
            throw new UnsupportedOperationException();

        return value.equals(((NumericValue)v).getNumericValue());
    }

    @Override
    public boolean lte(Value v) throws UnsupportedOperationException{
        if(! (v instanceof NumericValue))
            throw new UnsupportedOperationException();

        return value<=((NumericValue)v).getNumericValue().intValue();
    }

    @Override
    public boolean gte(Value v) throws UnsupportedOperationException{
        if(! (v instanceof NumericValue))
            throw new UnsupportedOperationException();

        return value>=((NumericValue)v).getNumericValue().intValue();
    }


}
