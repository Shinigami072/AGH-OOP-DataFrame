package lab0.dataframe.values;


import lab0.dataframe.exceptions.DFValueBuildException;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;

public abstract class Value<T> implements Cloneable {
    public Value operate(OPERATION_TYPES operation, Value operand) {
        switch (operation) {
            case ADD:
                return add(operand);
            case SUB:
                return sub(operand);
            case MUL:
                return mul(operand);
            case DIV:
                return div(operand);
            case POW:
                return pow(operand);
        }

        throw new UnsupportedOperationException();
    }

    public enum OPERATION_TYPES {
        ADD,
        SUB,
        MUL,
        DIV,
        POW
    }

    //Lazy loading Factories
    private static HashMap<Class<? extends Value>, ValueBuilder> factories;
    public static ValueBuilder builder(Class<? extends Value> c) {

        if (factories == null)
            factories = new HashMap<>();

        ValueBuilder f = factories.get(c);
        if (f == null) {
            f = new ValueBuilder(c);
            factories.put(c, f);
        }
        return f;
    }

    /**
     * getStoredValue
     *
     * @return Stored Value
     */
    public abstract Object getValue();

    /**
     * toString
     * @return String Representation
     */
    public String toString(){
        return getValue().toString();
    }

    /**
     * create
     *
     * @return Value parse string and create Value Representation
     */
    public abstract Value create(String s);

    /**
     * implementation dependent
     *
     * @return create new Value adding the two calues together
     */
    public abstract Value add(Value v) throws UnsupportedOperationException;

    /**
     * implementation dependent
     * @return create new Value subtracting the two calues together
     */
    public abstract Value sub(Value v) throws UnsupportedOperationException;

    /**
     * implementation dependent
     * @return create new Value multiplying the two calues together
     */
    public abstract Value mul(Value v) throws UnsupportedOperationException;

    /**
     * implementation dependent
     * @return create new Value dic=viding the two calues together
     */
    public abstract Value div(Value v) throws UnsupportedOperationException;

    /**
     * implementation dependent
     * @return create new Value being current Value to the power of the other
     */
    public abstract Value pow(Value v) throws UnsupportedOperationException;

    /**
     * implementation dependent
     * @return return if both values are equal, if vaLues are not of same type always false
     */
    public abstract boolean eq(Value v) throws UnsupportedOperationException;

    /**
     * implementation dependent
     * @return return if both values are less or  equal
     */
    public abstract boolean lte(Value v) throws UnsupportedOperationException;

    /**
     * implementation dependent
     * @return return if both values are greater or  equal
     */
    public abstract boolean gte(Value v) throws UnsupportedOperationException;

    /**
     * implementation dependent
     * @return return if both values are not  equal
     */
    public abstract boolean neq(Value v) throws UnsupportedOperationException;

    public boolean equals(Object other) {
        if (!(other instanceof Value))
            return false;
        else
            return getValue().equals(((Value) other).getValue());
    }

    public int hashCode() {
        return getValue().hashCode();
    }

    @Override
    public Value clone() throws CloneNotSupportedException {
        return (Value) super.clone();
    }

    public static class ValueBuilder {
        final Class<? extends Value> typ;
        Value val;

        ValueBuilder(Class<? extends Value> c) {
            typ = c;
            try {
                val = typ.getDeclaredConstructor().newInstance();
            } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
                e.printStackTrace();
            }
        }

        public Value build(String data) throws DFValueBuildException {
            try {
                return val.create(data);
            } catch (Exception e) {
                throw new DFValueBuildException("parseError: \"" + data + "\" into " + typ.getSimpleName() + " : " + e.getMessage());

            }
        }
    }
}
