package lab0.dataframe.values;

public abstract class NumericValue extends Value{
    abstract public Number getValue();

    /**
     * Comparison uses !eq
     *
     * @param v argument
     * @return is not equal
     */
    @Override
    public boolean neq(Value v){
        return !eq(v);
    }

    /**
     * Comparison - uses numeric equals
     *
     * @param v argument
     * @return is equal
     */
    @Override
    public boolean eq(Value v) {
        if (!(v instanceof NumericValue))
            throw new UnsupportedOperationException();
        NumericValue diff = (NumericValue)sub(v);
        double this_val = Math.abs(getValue().doubleValue());
        double other_val = Math.abs(((Number)(v.getValue())).doubleValue());
        double epsilon = 1.0e-13 * ( this_val>other_val? this_val : other_val);
        final DoubleValue positive_epsilon = new DoubleValue(epsilon);
        final DoubleValue negative_epsilon = new DoubleValue(-epsilon);

        return (diff.lte(positive_epsilon) &&diff.gte(negative_epsilon));
    }

    @Override
    public boolean equals(Object v) {
        if (!(v instanceof NumericValue))
            return false;

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
