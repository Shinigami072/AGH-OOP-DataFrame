package lab0.dataframe.values;

public abstract class NumericValue extends Value{

    abstract public Number getValue();


    /**
     * Comparison uses !eq
     *
     * @param v
     * @return
     */
    @Override
    public boolean neq(Value v){
        return !eq(v);
    }

    /**
     * Comparison - uses numeric equals
     *
     * @param v
     * @return
     */
    @Override
    public boolean eq(Value v) {
        if (!(v instanceof NumericValue))
            throw new UnsupportedOperationException();

        return getValue().equals(((NumericValue) v).getValue());
    }

    /**
     * Comparison, as if both values were double
     */
    @Override
    public boolean lte(Value v) {
        if (!(v instanceof NumericValue))
            throw new UnsupportedOperationException();

        return getValue().doubleValue() <= (((NumericValue) v).getValue().doubleValue());
    }

    /**
     * Comparison, as if both values were double
     */
    @Override
    public boolean gte(Value v) {
        if (!(v instanceof NumericValue))
            throw new UnsupportedOperationException();

        return getValue().doubleValue() >= (((NumericValue) v).getValue().doubleValue());
    }
}
