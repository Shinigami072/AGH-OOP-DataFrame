package lab0.dataframe.values;

import java.time.LocalDateTime;

public class DateTimeValue extends Value {

    private LocalDateTime val;

    DateTimeValue() {
    }

    public DateTimeValue(String value) {
        if(value.length()>16 || Character.isWhitespace(value.charAt(0)) || Character.isWhitespace(value.charAt(value.length()-1)))
            value=value.trim();
        if(value.length()<16){
//            value= String.format("%sT00:00:00", value);
            value+="T00:00:00";
        }

        val = LocalDateTime.parse(value);
    }

    /**
     * Box the value
     *
     * @param value value to store
     */
    public DateTimeValue(LocalDateTime value) {
        val = value;
    }


    @Override
    public LocalDateTime getValue() {
        return val;
    }

    @Override
    public String toString() {
        return val.toString();
    }

    @Override
    public DateTimeValue create(String value) {

        return new DateTimeValue(value);
    }


    /**
     * Unsupported
     *
     * @throws UnsupportedOperationException not implemented
     */
    @Override
    public DateTimeValue add(Value v) throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    /**
     * Unsupported
     *
     * @throws UnsupportedOperationException not implemented
     */
    @Override
    public DateTimeValue sub(Value v) throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    /**
     * Unsupported
     *
     * @throws UnsupportedOperationException not implemented
     */
    @Override
    public DateTimeValue mul(Value v) throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    /**
     * Unsupported
     *
     * @throws UnsupportedOperationException not implemented
     */
    @Override
    public DateTimeValue div(Value v) throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    /**
     * Unsupported
     *
     * @throws UnsupportedOperationException not implemented
     */
    @Override
    public DateTimeValue pow(Value v) throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    /**
     * checks it dateTile is equal
     */
    @Override
    public boolean eq(Value v) {
        if (v instanceof DateTimeValue)
            return getValue().equals(v.getValue());
        else
            return false;
    }

    /**
     * Checks if this date4 is before v
     *
     * @param v date to check with
     * @return if this ids before v
     */
    @Override
    public boolean lte(Value v) {
        if (v instanceof DateTimeValue)
            return getValue().isBefore(((DateTimeValue) v).getValue());
        else
            throw new UnsupportedOperationException();
    }

    /**
     * Checks if this date4 is after v
     *
     * @param v date to check with
     * @return if this ids after v
     */
    @Override
    public boolean gte(Value v) {
        if (v instanceof DateTimeValue)
            return getValue().isAfter(((DateTimeValue) v).getValue());
        else
            throw new UnsupportedOperationException();
    }


    /**
     * checks it dateTile is not equal
     */
    @Override
    public boolean neq(Value v) {
        return !eq(v);
    }


    @Override
    public DateTimeValue clone() throws CloneNotSupportedException {
        DateTimeValue dt = (DateTimeValue) super.clone();
        dt.val=LocalDateTime.from(val);
        return dt;
    }


}
