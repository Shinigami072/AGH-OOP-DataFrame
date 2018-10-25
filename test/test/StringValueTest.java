package test;

import lab0.dataframe.values.StringValue;
import lab0.dataframe.values.Value;
import org.junit.jupiter.api.BeforeEach;

class StringValueTest extends TESTValue{

    @BeforeEach
    void setUp() {
        int count = 10000;

        values = new Value[count];
        correct_values = new String[count];

        for (int i = 0; i < count; i++) {
            correct_values[i] = "ABCDEFRTHSWHTRWE"+i+"AVBREEDGEBTRagr".charAt(i%15);
            if (i<count/2)
                values[i] = new StringValue((String) correct_values[i]);
            else
                values[i]= Value.builder(StringValue.class).build(correct_values[i].toString());
        }
    }


}