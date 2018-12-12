package test;

import lab0.dataframe.DataFrame;
import lab0.dataframe.exceptions.DFColumnTypeException;
import lab0.dataframe.values.DoubleValue;
import org.junit.jupiter.api.BeforeEach;

public class KolumnOperationTest {

    DataFrame.Kolumna kol;
    @BeforeEach
    void setUp() {
        kol = new DataFrame.Kolumna("NAME", DoubleValue.class);
        try {

            for (int i = -10; i < 10; i++) {
                kol.add(new DoubleValue(i));

        }
        } catch (DFColumnTypeException e) {
            e.printStackTrace();
        }
    }

}
