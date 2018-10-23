package lab0.dataframe.values;


public abstract class Value {

    public abstract Object getValue();
    public abstract String toString();
    public abstract Value create(String s);

    //only work on same type fe. numeric type
    public abstract Value add(Value v);
    public abstract Value sub(Value v);
    public abstract Value mul(Value v);
    public abstract Value div(Value v);
    public abstract Value pow(Value v);

    public abstract boolean eq(Value v);
    public abstract boolean lte(Value v);
    public abstract boolean gte(Value v);
    public abstract boolean neq(Value v);

    //todo: public abstract boolean equals(Object other);
    //todo: public abstract int hashCode();
}
