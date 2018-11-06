package test;

import lab0.dataframe.values.StringValue;
import lab0.dataframe.values.Value;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import testDeps.TESTValue;

import static org.junit.jupiter.api.Assertions.assertEquals;

class StringValueTest extends TESTValue {

    @BeforeEach
    void setUp() {
        int count = 10000;

        values = new Value[count];
        correct_values = new String[count];

        for (int i = 0; i < count; i++) {
            correct_values[i] = "ABCDEFRTHSWHTRWE"+Integer.toString(i)+("AVBREEDGEBTRagr".charAt(i%15));
            if (i<count/2)
                values[i] = new StringValue((String) correct_values[i]);
            else
                values[i]= Value.builder(StringValue.class).build(correct_values[i].toString());
        }
    }


    @Test
    @Override
    public void Test_lte(){
        for (int j = 0; j < values.length; j++)
            for (int i = 0; i < values.length; i++) {
                assertEquals(((String)correct_values[i]).compareTo((String)correct_values[j])>=0, values[i].lte(values[j]));
            }
    }

    @Test
    @Override
    public void Test_gte() {
        for (int j = 0; j < values.length; j++)
            for (int i = 0; i < values.length; i++) {
                assertEquals(((String)correct_values[i]).compareTo((String)correct_values[j])<=0, values[i].gte(values[j]));
            }
    }

    @Test
    @Override
    public void Test_add() {
        for (int j = 0; j < values.length; j++)
            for (int i = 0; i < values.length; i++) {
                assertEquals(((String)correct_values[i] + (String)correct_values[j]), (values[i].add(values[j])).getValue());
            }
    }
}