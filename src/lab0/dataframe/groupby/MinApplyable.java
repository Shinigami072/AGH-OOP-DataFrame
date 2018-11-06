package lab0.dataframe.groupby;

import lab0.dataframe.DataFrame;
import lab0.dataframe.values.Value;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.SortedSet;

public class MinApplyable implements Applyable {

    @Override
    public DataFrame apply(DataFrame df) {
        DataFrame output = new DataFrame(df.getNames(), df.getTypes());

        HashSet<Integer> bannedColumns = new HashSet<>();

        int size = df.size();
        if (size > 0) {
            Value[] min = df.getRecord(0);

            for (int i = 1; i < size; i++) {
                Value[] row = df.getRecord(i);

                for (int kolumna = 0; kolumna < min.length; kolumna++) {

                    if (bannedColumns.contains(kolumna))
                        continue;

                    try {
                        if (row[kolumna].lte(min[kolumna]))
                            min[kolumna] = row[kolumna];
                    } catch (UnsupportedOperationException ignored) {
                        bannedColumns.add(kolumna);
                    }
                }

            }
            output.addRecord(min);
        }

        if (bannedColumns.size() == output.colCount())
            throw new RuntimeException("Really?1");

        String[] output_colnames = output.getNames();
        String[] colnames = new String[output.colCount() - bannedColumns.size()];

        for (int i = 0, j = 0; i < output_colnames.length; i++) {
            if (!bannedColumns.contains(i))
                colnames[j++] = output_colnames[i];
        }

        return output.get(colnames, false);
    }
}
