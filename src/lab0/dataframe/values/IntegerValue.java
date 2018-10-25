package lab0.dataframe.values;

public class IntegerValue extends NumericValue {

    private Integer value;

    IntegerValue() {
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

        return new IntegerValue(value + ((NumericValue) v).getValue().intValue());
    }

    @Override
    public IntegerValue sub(Value v) throws UnsupportedOperationException{
        if(! (v instanceof NumericValue))
            throw new UnsupportedOperationException();

        return new IntegerValue(value - ((NumericValue) v).getValue().intValue());
    }

    @Override
    public IntegerValue mul(Value v) throws UnsupportedOperationException{
        if(! (v instanceof NumericValue))
            throw new UnsupportedOperationException();

        return new IntegerValue(value * ((NumericValue) v).getValue().intValue());
    }

    @Override
    public IntegerValue div(Value v) throws UnsupportedOperationException{
        if(! (v instanceof NumericValue))
            throw new UnsupportedOperationException();

        return new IntegerValue(value / ((NumericValue) v).getValue().intValue());
    }

    /**
     * returns integer Value
     *
     * @param v is interpreted as double
     * @return this^v
     * @throws UnsupportedOperationException
     */
    @Override
    public IntegerValue pow(Value v) throws UnsupportedOperationException {
        if(! (v instanceof NumericValue))
            throw new UnsupportedOperationException();

        return new IntegerValue((int) Math.pow(value, ((NumericValue) v).getValue().doubleValue()));
    }

}
