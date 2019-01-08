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

    /**
     * Works only on numeric types,
     * the result of addition is cast to an double
     * @param v Value to add
     * @return DoubleValue containing result
     * @throws UnsupportedOperationException not implemented
     */
    @Override
    public DoubleValue add(Value v) {
        if(! (v instanceof NumericValue))
            throw new UnsupportedOperationException();

        return new DoubleValue(d + ((NumericValue) v).getValue().doubleValue());
    }

    /**
     * Works only on numeric types,
     * the result of subtraction is cast to an double
     * @param v Value to add
     * @return DoubleValue containing result
     * @throws UnsupportedOperationException not implemented
     */
    @Override
    public DoubleValue sub(Value v) {
        if (!(v instanceof NumericValue))
            throw new UnsupportedOperationException();

        return new DoubleValue(d - ((NumericValue) v).getValue().doubleValue());
    }

    /**
     * Works only on numeric types,
     * the result of multiplication is cast to an double
     * @param v Value to add
     * @return DoubleValue containing result
     * @throws UnsupportedOperationException not implemented
     */
    @Override
    public DoubleValue mul(Value v) {
        if (!(v instanceof NumericValue))
            throw new UnsupportedOperationException();

        return new DoubleValue(d * ((NumericValue) v).getValue().doubleValue());
    }

    /**
     * Works only on numeric types,
     * the result of division is cast to an double
     * @param v Value to add
     * @return DoubleValue containing result
     * @throws UnsupportedOperationException not implemented
     */
    @Override
    public DoubleValue div(Value v) {
        if (!(v instanceof NumericValue))
            throw new UnsupportedOperationException();

        if (((NumericValue) v).getValue().doubleValue() == 0.0)
            throw new ArithmeticException("divided by 0");

        return new DoubleValue(d / ((NumericValue) v).getValue().doubleValue());
    }

    /**
     * Works only on numeric types,
     * the result of this to the power of argument is cast to an double
     * @param v Value to add
     * @return DoubleValue containing result
     * @throws UnsupportedOperationException not implemented
     */
    @Override
    public DoubleValue pow(Value v) {
        if (!(v instanceof NumericValue))
            throw new UnsupportedOperationException();

        return new DoubleValue(Math.pow(d, ((NumericValue) v).getValue().doubleValue()));
    }
}
