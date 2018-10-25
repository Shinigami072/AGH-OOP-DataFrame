package lab0.dataframe.values;

public class DoubleValue extends NumericValue {
    private Double d;

    DoubleValue() {
    }
    public DoubleValue(double value){
        d=value;
    }

    @Override
    public Double getValue() {
        return d;
    }

    @Override
    public String toString() {
        return d.toString();
    }

    @Override
    public DoubleValue create(String s) {
        return new DoubleValue(Double.parseDouble(s));
    }

    @Override
    public DoubleValue add(Value v) {
        if(! (v instanceof NumericValue))
            throw new UnsupportedOperationException();

        return new DoubleValue(d + ((NumericValue) v).getValue().doubleValue());
    }

    @Override
    public DoubleValue sub(Value v) {
        if (!(v instanceof NumericValue))
            throw new UnsupportedOperationException();

        return new DoubleValue(d - ((NumericValue) v).getValue().doubleValue());
    }

    @Override
    public DoubleValue mul(Value v) {
        if (!(v instanceof NumericValue))
            throw new UnsupportedOperationException();

        return new DoubleValue(d * ((NumericValue) v).getValue().doubleValue());
    }

    @Override
    public DoubleValue div(Value v) {
        if (!(v instanceof NumericValue))
            throw new UnsupportedOperationException();

        return new DoubleValue(d / ((NumericValue) v).getValue().doubleValue());
    }

    @Override
    public DoubleValue pow(Value v) {
        if (!(v instanceof NumericValue))
            throw new UnsupportedOperationException();

        return new DoubleValue(Math.pow(d, ((NumericValue) v).getValue().doubleValue()));
    }
}
