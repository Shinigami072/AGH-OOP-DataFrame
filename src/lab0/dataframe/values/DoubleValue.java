package lab0.dataframe.values;

public class DoubleValue extends NumericValue {
    Double d;
    public DoubleValue(double value){
        d=value;
    }
    @Override
    public Double getNumericValue() {
        return d;
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

        return new DoubleValue(d+((NumericValue)v).getNumericValue().doubleValue());
    }

    @Override
    public DoubleValue sub(Value v) {
        return null;
    }

    @Override
    public DoubleValue mul(Value v) {
        return null;
    }

    @Override
    public DoubleValue div(Value v) {
        return null;
    }

    @Override
    public DoubleValue pow(Value v) {
        return null;
    }

    @Override
    public boolean eq(Value v) {
        return false;
    }

    @Override
    public boolean lte(Value v) {
        return false;
    }

    @Override
    public boolean gte(Value v) {
        return false;
    }
}
