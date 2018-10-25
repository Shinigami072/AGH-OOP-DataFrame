package test;

import lab0.dataframe.values.DoubleValue;
import lab0.dataframe.values.IntegerValue;
import lab0.dataframe.values.Value;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;



class DoubleValueTest extends TESTValue {


        @BeforeEach
        void setUp() {
            int count = 20000;

            values = new Value[count];
            correct_values = new Double[count];

            for (int i = 0; i < count; i++) {
                correct_values[i] = (int) (Math.random() * count - count / 2);
                if (i<count/2)
                    values[i] = new DoubleValue((Double) correct_values[i]);
                else
                    values[i]= Value.builder(DoubleValue.class).build(correct_values[i].toString());
            }
        }

        //    @Test
//    void Test_toString() {
//        for (int i = 0; i < values.length; i++) {
//            assertEquals(Integer.toString(int_values[i]), values[i].toString());
//        }
//
//    }
//
//    @Test
//    void Test_equals() {
//        for (int j = 0; j < values.length; j++)
//            for (int i = 0; i < values.length; i++) {
//                assertEquals((int_values[i]).equals(int_values[j]), values[i].equals(values[j]));
//            }
//    }
//
//    @Test
//    void Test_hashCode() {
//        for (int i = 0; i < values.length; i++) {
//            assertEquals(Integer.hashCode(int_values[i]), values[i].hashCode());
//        }
//    }
//
//    @Test
//    void Test_getValue() {
//        for (int i = 0; i < values.length; i++) {
//            assertEquals(int_values[i], values[i].getValue());
//        }
//    }
//
//    @Test
//    void Test_create() {
//        for (int i = 0; i < values.length; i++) {
//            assertEquals(values[i], Value.builder(IntegerValue.class).build(int_values[i].toString()));
//        }
//    }
//
//    @Test
//    void Test_neq() {
//        for (int j = 0; j < values.length; j++)
//            for (int i = 0; i < values.length; i++) {
//                assertEquals(!(int_values[i]).equals(int_values[j]), values[i].neq(values[j]));
//            }
//    }
//
//    @Test
//    void Test_eq() {
//        for (int j = 0; j < values.length; j++)
//            for (int i = 0; i < values.length; i++) {
//                assertEquals((int_values[i]).equals(int_values[j]), values[i].eq(values[j]));
//            }
//    }
//
        @Test
        void Test_lte() {
            for (int j = 0; j < values.length; j++)
                for (int i = 0; i < values.length; i++) {
                    assertEquals(((Double)correct_values[i]) <= ((Double)correct_values[j]), values[i].lte(values[j]));
                }
        }

        @Test
        void Test_gte() {
            for (int j = 0; j < values.length; j++)
                for (int i = 0; i < values.length; i++) {
                    assertEquals(((Double)correct_values[i]) >= ((Double)correct_values[j]), values[i].gte(values[j]));
                }
        }

        @Test
        void Test_pow() {
            for (int j = 0; j < values.length; j++)
                for (int i = 0; i < values.length; i++) {
                    assertEquals(new DoubleValue((double) Math.pow(((Double)correct_values[i]), ((Double)correct_values[j]))), values[i].pow(values[j]));
                }
        }


        @Test
        void Test_add() {
            for (int j = 0; j < values.length; j++)
                for (int i = 0; i < values.length; i++) {
                    assertEquals(new DoubleValue((((Double)correct_values[i]) + ((Double)correct_values[j]))), values[i].add(values[j]));
                }
        }

        @Test
        void Test_sub() {
            for (int j = 0; j < values.length; j++)
                for (int i = 0; i < values.length; i++) {
                    assertEquals(new DoubleValue((((Double)correct_values[i]) - ((Double)correct_values[j]))), values[i].sub(values[j]));
                }
        }

        @Test
        void Test_mul() {
            for (int j = 0; j < values.length; j++)
                for (int i = 0; i < values.length; i++) {
                    assertEquals(new DoubleValue((((Double)correct_values[i]) * ((Double)correct_values[j]))), values[i].mul(values[j]));
                }
        }

        @Test
        void Test_div() {
            for (int j = 0; j < values.length; j++)
                for (int i = 0; i < values.length; i++) {
                    if ((Double)correct_values[j] != 0)
                        assertEquals(new DoubleValue((((Double)correct_values[i]) / ((Double)correct_values[j]))), values[i].div(values[j]));
                }
        }


    }
}
