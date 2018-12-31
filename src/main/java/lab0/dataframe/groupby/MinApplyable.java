package lab0.dataframe.groupby;

import lab0.dataframe.DataFrame;
import lab0.dataframe.exceptions.DFApplyableException;
import lab0.dataframe.exceptions.DFColumnTypeException;
import lab0.dataframe.values.Value;

import java.util.HashSet;
import java.util.Set;

public class MinApplyable extends ComparableApplyable {

    protected void comparison(Value[] optimal, Value[] tested, Set<Integer> banned) {
        for (int kolumna = 0; kolumna < optimal.length; kolumna++) {

            if (banned.contains(kolumna))
                continue;

            try {

                if (tested[kolumna].lte(optimal[kolumna]))
                    optimal[kolumna] = tested[kolumna];
            } catch (UnsupportedOperationException ignored) {
                banned.add(kolumna);
            }
        }
    }
}
