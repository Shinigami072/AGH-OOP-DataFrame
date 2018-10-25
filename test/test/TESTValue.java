package test;

import lab0.dataframe.values.Value;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TESTValue {

    Value[] values;

    Object[] correct_values;

    @Test
    void Test_toString() {
        for (int i = 0; i < values.length; i++) {
            assertEquals((correct_values[i]).toString(), values[i].toString());
        }

    }

    @Test
    void Test_equals() {
        for (int j = 0; j < values.length; j++)
            for (int i = 0; i < values.length; i++) {
                assertEquals((correct_values[i]).equals(correct_values[j]), values[i].equals(values[j]));
            }
    }

    @Test
    void Test_hashCode() {
        for (int i = 0; i < values.length; i++) {
            assertEquals((correct_values[i]).hashCode(), values[i].hashCode());
        }
    }

    @Test
    void Test_getValue() {
        for (int i = 0; i < values.length; i++) {
            assertEquals(correct_values[i], values[i].getValue());
        }
    }

    @Test
    void Test_create() {
        for (int i = 0; i < values.length; i++) {
            assertEquals(values[i], Value.builder(values[0].getClass()).build(correct_values[i].toString()));
        }
    }

    @Test
    void Test_neq() {
        for (int j = 0; j < values.length; j++)
            for (int i = 0; i < values.length; i++) {
                assertEquals(!(correct_values[i]).equals(correct_values[j]), values[i].neq(values[j]));
            }
    }

    @Test
    void Test_eq() {
        for (int j = 0; j < values.length; j++)
            for (int i = 0; i < values.length; i++) {
                assertEquals((correct_values[i]).equals(correct_values[j]), values[i].eq(values[j]));
            }
    }

    @Test
    void Test_lte() {

        assertThrows(UnsupportedOperationException.class,
                () -> {
                    for (Value value : values)
                        for (Value value1 : values)
                            value1.lte(value);
                }
        );

    }

    @Test
    void Test_gte() {
        assertThrows(UnsupportedOperationException.class,
                () -> {
                    for (Value value : values)
                        for (Value value1 : values) value1.gte(value);
                }
        );
    }

    @Test
    void Test_pow() {
        assertThrows(UnsupportedOperationException.class,
                () -> {
                    for (Value value : values)
                        for (Value value1 : values) value1.pow(value);
                }
        );
    }


    @Test
    void Test_add() {
        assertThrows(UnsupportedOperationException.class,
                () -> {
                    for (Value value : values)
                        for (Value value1 : values) value1.add(value);
                }
        );
    }

    @Test
    void Test_sub() {
        assertThrows(UnsupportedOperationException.class,
                () -> {
                    for (Value value : values)
                        for (Value value1 : values) value1.sub(value);
                }
        );
    }

    @Test
    void Test_mul() {
        assertThrows(UnsupportedOperationException.class,
                () -> {
                    for (Value value : values)
                        for (Value value1 : values) value1.mul(value);
                }
        );
    }

    @Test
    void Test_div() {
        assertThrows(UnsupportedOperationException.class,
                () -> {
                    for (Value value1 : values) for (Value value : values) value.div(value1);
                }
        );
    }
}
