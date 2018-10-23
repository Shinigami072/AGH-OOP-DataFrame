package lab0.dataframe.values;

public class StringValue extends Value {

    private String val;

    public StringValue(String value){
        val=value;
    }

    @Override
    public String getValue() {
        return val;
    }

    @Override
    public String toString() {
        return val;
    }

    @Override
    public StringValue create(String value) {
        return new StringValue(value);
    }

    @Override
    public StringValue add(Value v) {
        return new StringValue(val+v.toString());

    }

    @Override
    public StringValue sub(Value v) throws UnsupportedOperationException{
        throw new UnsupportedOperationException();
    }

    @Override
    public StringValue mul(Value v) throws UnsupportedOperationException{
        throw new UnsupportedOperationException();
    }

    @Override
    public StringValue div(Value v) throws UnsupportedOperationException{
        throw new UnsupportedOperationException();
    }

    @Override
    public StringValue pow(Value v) throws UnsupportedOperationException{
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean eq(Value v) {
        if(v instanceof StringValue)
            return eq(v);
        else
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

    @Override
    public boolean neq(Value v) {
        return !eq(v);
    }
}
