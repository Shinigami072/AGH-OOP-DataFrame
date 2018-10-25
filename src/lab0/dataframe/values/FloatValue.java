package lab0.dataframe.values;

public class FloatValue extends NumericValue {

    private Float value;

    FloatValue() {
    }

    public FloatValue(float i) {
        value = i;
    }

    @Override
    public Float getValue() {
        return value;
    }

    @Override
    public String toString() {
        return value.toString();
    }

    @Override
    public FloatValue create(String s) {
        return new FloatValue(Float.parseFloat(s));
    }

    /**
     * Works only on numeric types,
     * the result of addition is cast to an float
     * @param v Value to add
     * @return FloatValue containing result
     * @throws UnsupportedOperationException
     */
    @Override
    public FloatValue add(Value v) throws UnsupportedOperationException {
        if (!(v instanceof NumericValue))
            throw new UnsupportedOperationException();

        return new FloatValue(value + ((NumericValue) v).getValue().floatValue());
    }

    /**
     * Works only on numeric types,
     * the result of subtraction is cast to an float
     * @param v Value to add
     * @return FloatValue containing result
     * @throws UnsupportedOperationException
     */
    @Override
    public FloatValue sub(Value v) throws UnsupportedOperationException {
        if (!(v instanceof NumericValue))
            throw new UnsupportedOperationException();

        return new FloatValue(value - ((NumericValue) v).getValue().floatValue());
    }
    /**
     * Works only on numeric types,
     * the result of multiplication is cast to an float
     * @param v Value to add
     * @return FloatValue containing result
     * @throws UnsupportedOperationException
     */
    @Override
    public FloatValue mul(Value v) throws UnsupportedOperationException {
        if (!(v instanceof NumericValue))
            throw new UnsupportedOperationException();

        return new FloatValue(value * ((NumericValue) v).getValue().floatValue());
    }
    /**
     * Works only on numeric types,
     * the result of divison is cast to an float
     * @param v Value to add
     * @return FloatValue containing result
     * @throws UnsupportedOperationException
     */
    @Override
    public FloatValue div(Value v) throws UnsupportedOperationException {
        if (!(v instanceof NumericValue))
            throw new UnsupportedOperationException();

        return new FloatValue(value / ((NumericValue) v).getValue().floatValue());
    }

    /**
     * Works only on numeric types,
     * the result of power is cast to an float
     * @param v Value to add
     * @return FloatValue containing result
     * @throws UnsupportedOperationException
     */
    @Override
    public FloatValue pow(Value v) throws UnsupportedOperationException {
        if (!(v instanceof NumericValue))
            throw new UnsupportedOperationException();

        return new FloatValue((float) Math.pow(value, ((NumericValue) v).getValue().floatValue()));
    }

}
