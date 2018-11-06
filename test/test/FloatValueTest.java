package test;

import lab0.dataframe.values.DoubleValue;
import lab0.dataframe.values.Value;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import testDeps.TESTValue;

import static org.junit.jupiter.api.Assertions.assertEquals;


class FloatValueTest extends TESTValue {


        @BeforeEach
        public void setUp() {
            int count = 20000;

            values = new Value[count];
            correct_values = new Double[count];

            for (int i = 0; i < count; i++) {
                correct_values[i] = (Math.random() * count - count / 2);
                if (i<count/2)
                    values[i] = new DoubleValue((Double) correct_values[i]);
                else
                    values[i]= Value.builder(DoubleValue.class).build(correct_values[i].toString());
            }
        }

        @Test
        public void Test_lte() {
            for (int j = 0; j < values.length; j++)
                for (int i = 0; i < values.length; i++) {
                    assertEquals(((Double)correct_values[i]) <= ((Double)correct_values[j]), values[i].lte(values[j]));
                }
        }

        @Test
        public void Test_gte() {
            for (int j = 0; j < values.length; j++)
                for (int i = 0; i < values.length; i++) {
                    assertEquals(((Double)correct_values[i]) >= ((Double)correct_values[j]), values[i].gte(values[j]));
                }
        }

        @Test
        public void Test_pow() {
            for (int j = 0; j < values.length; j++)
                for (int i = 0; i < values.length; i++) {
                    assertEquals(new DoubleValue(Math.pow(((Double) correct_values[i]), ((Double) correct_values[j]))), values[i].pow(values[j]));
                }
        }


        @Test
        public void Test_add() {
            for (int j = 0; j < values.length; j++)
                for (int i = 0; i < values.length; i++) {
                    assertEquals(new DoubleValue((((Double)correct_values[i]) + ((Double)correct_values[j]))), values[i].add(values[j]));
                }
        }

        @Test
        public void Test_sub() {
            for (int j = 0; j < values.length; j++)
                for (int i = 0; i < values.length; i++) {
                    assertEquals(new DoubleValue((((Double)correct_values[i]) - ((Double)correct_values[j]))), values[i].sub(values[j]));
                }
        }

        @Test
        public void Test_mul() {
            for (int j = 0; j < values.length; j++)
                for (int i = 0; i < values.length; i++) {
                    assertEquals(new DoubleValue((((Double)correct_values[i]) * ((Double)correct_values[j]))), values[i].mul(values[j]));
                }
        }

        @Test
        public void Test_div() {
            for (int j = 0; j < values.length; j++)
                for (int i = 0; i < values.length; i++) {
                    if ((Double)correct_values[j] != 0)
                        assertEquals(new DoubleValue((((Double)correct_values[i]) / ((Double)correct_values[j]))), values[i].div(values[j]));
                }
        }


    }

