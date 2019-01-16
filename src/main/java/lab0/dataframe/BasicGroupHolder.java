package lab0.dataframe;

import lab0.dataframe.exceptions.DFApplyableException;
import lab0.dataframe.exceptions.DFColumnTypeException;
import lab0.dataframe.groupby.Applyable;
import lab0.dataframe.groupby.GroupBy;
import lab0.dataframe.values.Value;

import java.util.*;

//todo: possible memory improvement - store groups as lists of rows in original DataFrame
public final class BasicGroupHolder implements GroupBy {

    private final LinkedList<DataFrame> groups;
    private final String[] data_column_names;
    private final DataFrame id_values;


    BasicGroupHolder(Collection<DataFrame> collection, String[] column_names, Class<? extends Value>[] types) {
        groups = new LinkedList<>(collection);


        String[] all_column_names = groups.getFirst().getNames();

        Set<String> all = new HashSet<>(Arrays.asList(all_column_names));
        all.removeAll(Arrays.asList(column_names));
        data_column_names = all.toArray(new String[0]);
        id_values = new DataFrame(column_names, types);

        try {

            for (DataFrame df : groups) {
                Value[] row = new Value[column_names.length];

                for (int i = 0; i < column_names.length; i++) {

                    row[i] = (df.get(column_names[i]).get(0));

                }

                id_values.addRecord(row);
            }

        } catch (DFColumnTypeException e) {
            e.printStackTrace();
        }


    }

    public List<DataFrame> getGroups() {
        return groups;
    }

    @Override
    public DataFrame apply(Applyable function) throws DFApplyableException {

        try {
            DataFrame output = null;
            for (int groupID = 0; groupID < groups.size(); groupID++) {

                DataFrame group = function.apply(groups.get(groupID).get(data_column_names, false));
                //inicjalizacja DataFrame output
                //tak żeby zawierał odpowiednie typy kolumn na wyjściu
                if (output == null) {
                    output = GroupBy.getOutputDataFrame(id_values.getTypes(), id_values.getNames(), group.getTypes(), group.getNames());
                }

                //przepisanie wartości z temp, jeżeli coś zawiera
                if (group.size() > 0) {
                    GroupBy.addGroup(output, id_values.getRecord(groupID), group);
                }

            }


            return output;

        } catch (DFColumnTypeException | CloneNotSupportedException e) {
            throw new DFApplyableException(e.getMessage());
        }

    }
    public String toString(){
        StringBuilder builder = new StringBuilder("Groups:\n");
        int i=0;
        for (DataFrame d:groups) {
            builder.append("Group:").append(Arrays.toString(id_values.getRecord(i++))).append('\n');
            builder.append(d).append('\n');
        }

        return builder.toString();
    }


}
