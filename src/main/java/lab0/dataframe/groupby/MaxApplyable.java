package lab0.dataframe.groupby;

import lab0.dataframe.values.Value;

import java.util.Set;

public class MaxApplyable extends ComparableApplyable {

    protected void comparison(Value[] optimal, Value[] tested, Set<Integer> banned) {
        for (int kolumna = 0; kolumna < optimal.length; kolumna++) {

            if (banned.contains(kolumna))
                continue;

            try {

                if (tested[kolumna].gte(optimal[kolumna]))
                    optimal[kolumna] = tested[kolumna];
            } catch (UnsupportedOperationException ignored) {
                banned.add(kolumna);
            }
        }
    }
}
