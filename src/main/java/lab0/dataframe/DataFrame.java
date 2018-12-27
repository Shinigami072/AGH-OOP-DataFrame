package lab0.dataframe;

import lab0.dataframe.exceptions.*;
import lab0.dataframe.groupby.Applyable;
import lab0.dataframe.groupby.GroupBy;
import lab0.dataframe.values.Value;

import java.io.*;
import java.util.*;

public class DataFrame {
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////
    protected Column[] columns;

    //file loading constructors -
    //true - file contains column names
    public DataFrame(String path, Class<? extends Value>[] column_type) throws IOException, DFColumnTypeException, DFValueBuildException {
        this(path, column_type, null);
    }
    protected int rowNumber;

    //false - file does not contain column names
    public DataFrame(String path, Class<? extends Value>[] column_type, String[] column_name) throws IOException, DFColumnTypeException, DFValueBuildException {
        this(column_type.length);
        boolean header = column_name == null;
        for (int i = 0; i < column_type.length; i++)
            columns[i] = new Column(header ? "" : column_name[i], column_type[i]);
        readFile(path, header);
    }

    protected DataFrame(int count) throws DFZeroLengthCreationException {
        if (count == 0)
            throw new DFZeroLengthCreationException();
        columns = new Column[count];
        rowNumber = 0;

    }

    public DataFrame(Column[] columns) throws DFZeroLengthCreationException {
        if (columns.length == 0)
            throw new DFZeroLengthCreationException();
        this.columns = columns;
        rowNumber = columns[0].size();
    }

    public DataFrame(String[] nazwyKolumn, Class<? extends Value>[] typyKolumn) {
        this(nazwyKolumn.length);
        for (int i = 0; i < typyKolumn.length; i++)
            columns[i] = new Column(nazwyKolumn[i], typyKolumn[i]);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////
    protected void readFile(String path, boolean header) throws IOException, DFColumnTypeException, DFValueBuildException {
        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            String[] strLine;
            if (header) {
                strLine = br.readLine().split(",");
                for (int i = 0; i < columns.length; i++) {
                    columns[i].nazwa = strLine[i];
                }
            }

            String temp;


            Value[] tempValues = new Value[columns.length];
            Value.ValueBuilder[] builders = new Value.ValueBuilder[columns.length];
            for (int i = 0; i < columns.length; i++) {
                builders[i] = Value.builder(columns[i].typ);
            }

            while ((temp = br.readLine()) != null) {

                strLine = temp.split(",");
                int i = 0;
                for (String s : strLine) {
                    tempValues[i] = builders[i].build(s);//todo: malformed Data Exception
                    i++;
                }
                addRecord(tempValues);
            }
        }
    }

    /**
     * Get DataFrame o danych kolumnach
     *
     * @param cols nazwy kolumn
     * @param copy wykonanie głębokiej kopii
     * @return podzbiór dF
     */
    public DataFrame get(String[] cols, boolean copy) throws DFZeroLengthCreationException, CloneNotSupportedException {
        Column[] kolumny = new Column[cols.length];

        for (int i = 0; i < cols.length; i++)
            if (copy) {
                kolumny[i] = get(cols[i]).copy();
            } else
                kolumny[i] = get(cols[i]);

        return new DataFrame(kolumny);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////
    public int size(){
        return rowNumber;
    }

    public String[] getNames(){
        String[] names = new String[columns.length];
        for (int i = 0; i < columns.length; i++)
            names[i] = columns[i].nazwa;
        return names;
    }

    public Class<? extends Value>[] getTypes(){
        //noinspection unchecked
        Class<? extends Value>[] types = (Class<? extends Value>[]) (new Class[columns.length]);
        for (int i = 0; i < columns.length; i++)
            types[i] = columns[i].typ;
        return types;
    }

    /**
     * getter columns o danej nazwie
     * zwraca pierwsza kolumnę o danej nazwie
     * @param colname nazwa columns
     * @return kolumna
     */
    //todo: use map for faster get
    public Column get(String colname) {
        for (Column k : columns)
            if(k.nazwa.equals(colname))
                return  k;

        throw new NoSuchElementException("No such column: "+colname);
    }

    /**
     * Dodanie rekordu do DataFrame
     * @param values elementy rekordu
     */
    public void addRecord(Value... values) throws DFColumnTypeException, DFDimensionException {
        if (values.length != columns.length)
            throw new DFDimensionException(String.format("DF col %d , record length: %d", getColCount(), values.length));
        for (int i = 0; i < columns.length; i++)
            if (!columns[i].typ.isInstance(values[i]))
                throw new DFColumnTypeException(columns[i], values[i], i);

        int i=0;
        rowNumber++;
        for (Column k : columns)
            k.add(values[i++]);

    }

    public int getColCount() {
        return columns.length;
    }

    /**
     * Zwraca wiersz jako Array obiektów
     * @param i nr.wiersz
     * @return wiersz
     */
    public Value[] getRecord(int i) {
        if (i < 0 || i > size())
            throw new DFIndexOutOfBounds("Out of bounds: "+i);
        Value[] temp = new Value[columns.length];
        int j=0;
        for (Column k : columns)
            temp[j++]=k.get(i);

        return temp;
    }

    /**
     * Zwraca wiersz jako DataFrame
     * @param i nr wiersza
     * @return Wiersz
     */
    public DataFrame iloc(int i) throws DFColumnTypeException {

        return iloc(i,i);
    }

    /**
     * Zwraca wiersze jako DataFrame
     * @param from od
     * @param to  do
     * @return Wiersze
     */
    public DataFrame iloc(int from, int to) throws DFColumnTypeException{
        checkBounds(from, to);
        String[] nazwy = new String[columns.length];
        @SuppressWarnings("unchecked") Class<? extends Value>[] typy = (Class<? extends Value>[]) (new Class[columns.length]);
        for (int i = 0; i < columns.length; i++) {
            nazwy[i] = columns[i].nazwa;
            typy[i] = columns[i].typ;
        }

        DataFrame df = new DataFrame(nazwy, typy);

        for (int i = from; i <= to; i++) {
            df.addRecord(getRecord(i));
        }

        return df;
    }

    protected void checkBounds(int from, int to) {
        if(from<0 || from>=rowNumber)
            throw new DFIndexOutOfBounds("No such index: "+from);

        if(to<0 || to>=rowNumber)
            throw new DFIndexOutOfBounds("No such index: "+to);

        if(to < from)
            throw new DFIndexOutOfBounds("unable to create range from " + from + " to "+to);
    }
    public GroupBy groupBy(String... colname) throws CloneNotSupportedException {

        class ValueGroup implements Comparable<ValueGroup>{
            private Value[] id;

            private ValueGroup(Value[] key){
                id=key;
            }

            @Override
            public boolean equals(Object o) {
                if(o instanceof ValueGroup) {
                    ValueGroup other=(ValueGroup)o;
                    if(id.length!= other.id.length)
                        return false;
                    return Arrays.deepEquals(id,other.id);
                } else
                    return false;
            }

            @Override
            public int hashCode() {

                return Arrays.hashCode(id);
            }

            @Override
            public int compareTo(ValueGroup valueGroup) {
                for (int i = 0; i < id.length; i++) {
                    if(id[i].equals(valueGroup.id[i]))
                        continue;
                    if(id[i].lte(valueGroup.id[i]))
                        return -1;
                    else
                        return 1;
                }
                return 0;
            }
        }


        Hashtable<ValueGroup,DataFrame> storage =  new Hashtable<>();
        DataFrame keys = get(colname,false);
        try {
            for (int i = 0; i < size(); i++) {
                ValueGroup key = new ValueGroup(keys.getRecord(i));
                DataFrame group = storage.get(key);
                if (group == null) {
                    group = new SparseDataFrame(getNames(), getRecord(i));
                    storage.put(key, group);
                }
                group.addRecord(getRecord(i));
            }

        } catch (DFColumnTypeException e) {
            e.printStackTrace();
        }

        return new Grupator4000(new TreeMap<>(storage).values(), colname);//todo - czech perfofmans pls

    }


    @Override
    public String toString() {
        StringBuilder s = new StringBuilder();
        String[] str;
        for (Column k : columns) {
            s.append(String.format("|%-14.14s:%15.15s", k.nazwa, k.typ.getSimpleName()));
        }
        s.append("|\n");
        for (int i = 0; i < rowNumber; i++) {
            for (Column k : columns)
                s.append(String.format("|%30.30s", k.get(i).toString()));
            s.append("|\n");
        }
        return s.toString();
    }

    /**
     * Data container
     */
    public static class Column {
        final ArrayList<Value> dane;

        /**
         *
         * @param nazwa Kolumny
         * @param typ Przechowywany typ danych
         */
        public Column(String nazwa, Class<? extends Value> typ) {
            dane = new ArrayList<>();
            this.nazwa = nazwa;
            this.typ = typ;
        }

        String nazwa;
        final Class<? extends Value> typ;

        /**
         * Kopiowanie
         * @param source kolumna do skopiowania
         */
        Column(Column source) throws CloneNotSupportedException {

            this.nazwa = source.nazwa;
            this.typ = source.typ;
            dane = new ArrayList<>();
            try {
                for (Value o : source.dane) {
                    add(o.clone());
                }
            } catch (DFColumnTypeException e) {
                e.printStackTrace();
            }

        }

        public String getName() {
            return nazwa;
        }


        /**
         * Accessor
         * @param index wiersz
         * @return Obiekt w wierszu I
         */
        public Value get(int index) {
            return dane.get(index);
        }

        /**
         *
         * @param o nowy wiersz
         */
        public void add(Value o) throws DFColumnTypeException {
            if (typ.isInstance(o))
                dane.add(o);
            else
                throw new DFColumnTypeException(this, o, size());
        }

        /**
         *
         * @return ilość elementów
         */
        public int size() {
            return dane.size();
        }

        @Override
        public String toString() {

            StringBuilder s = new StringBuilder();
            String[] str = typ.getTypeName().split("\\.");
            s.append(String.format("|%-14.14s:%15.15s", nazwa, str[str.length - 1]));
            s.append("|\n");
            for (int i = 0; i < size(); i++) {
                s.append(String.format("|%30.30s|\n", get(i).toString()));
            }
            return s.toString();
        }

        public Column copy() throws CloneNotSupportedException {
            return new Column(this);
        }

        public int uniqueSize() {
            return size();
        }

        @Override
        public int hashCode() {
            int hash = 0;
            for (int i = 0; i < size(); i++) {
                hash += get(i).hashCode();
            }
            return hash;
        }

        protected Column performOperation(Value.OPERATION_TYPES operation, Value operand) throws DFColumnTypeException {
            Column output = new Column(getName(), (get(0).operate(operation, operand)).getClass());

            for (Value v : dane) {
                output.add(v.operate(operation, operand));
            }

            return output;
        }

        protected Column performOperation(Value.OPERATION_TYPES operation, Column operand) throws DFColumnTypeException {
            if (operand.size() != size())
                throw new DFDimensionException(
                        String.format("kolumn %s (%d) , kolumn %s (%d) have different length",
                                getName(), size(),
                                operand.getName(), operand.size()));

            Column output = new Column(getName(), get(0).operate(operation, operand.get(0)).getClass());

            for (int i = 0; i < size(); i++) {
                output.add(get(i).operate(operation, operand.get(i)));
            }


            return output;
        }

        public Column v_add(Value v) throws DFColumnTypeException {
            return performOperation(Value.OPERATION_TYPES.ADD, v);
        }

        public Column v_sub(Value v) throws DFColumnTypeException {
            return performOperation(Value.OPERATION_TYPES.SUB, v);
        }

        public Column v_mul(Value v) throws DFColumnTypeException {
            return performOperation(Value.OPERATION_TYPES.MUL, v);
        }

        public Column v_div(Value v) throws DFColumnTypeException {
            return performOperation(Value.OPERATION_TYPES.DIV, v);
        }

        public Column v_pow(Value v) throws DFColumnTypeException {
            return performOperation(Value.OPERATION_TYPES.POW, v);
        }

        public Column v_add(Column k) throws DFColumnTypeException {
            return performOperation(Value.OPERATION_TYPES.ADD, k);
        }

        public Column v_sub(Column k) throws DFColumnTypeException {
            return performOperation(Value.OPERATION_TYPES.SUB, k);
        }

        public Column v_mul(Column k) throws DFColumnTypeException {
            return performOperation(Value.OPERATION_TYPES.MUL, k);
        }

        public Column v_div(Column k) throws DFColumnTypeException {
            return performOperation(Value.OPERATION_TYPES.DIV, k);
        }

        public Column v_pow(Column k) throws DFColumnTypeException {
            return performOperation(Value.OPERATION_TYPES.POW, k);
        }


        @Override
        public boolean equals(Object obj) {
            if (obj instanceof Column) {
                Column k = (Column) obj;

                if (k.size() != size())
                    return false;

                for (int i = 0; i < size(); i++) {
                    if (!k.get(i).eq(get(i))) {
                        return false;
                    }
                }

                return true;
            }
            return false;
        }

        public Class<? extends Value> getType() {
            return typ;
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof DataFrame) {
            DataFrame df = (DataFrame) obj;
            return Arrays.deepEquals(columns, df.columns);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(columns);
    }

    //todo: possible memory improvement - store groups as lists of rows in original DataFrame
    public final class Grupator4000 implements GroupBy {

        private final LinkedList<DataFrame> groups;
        private final String[] id_colnames;
        private final String[] data_colnames;

        private final Column[] id_columns;//todo- possible improvement


        public Grupator4000(Collection<DataFrame> collection, String[] colnames) {
            groups = new LinkedList<>(collection);
            id_colnames=colnames;
            id_columns = new Column[colnames.length];

            String[] all_colnames = groups.getFirst().getNames();
            data_colnames= new String[all_colnames.length-id_colnames.length];

            int j =0;
            outer:
            for (String colname : all_colnames) {

                //setn containing all_colnames -excluding id colnames- used for faster access to data
                for (String id : id_colnames)
                    if (colname.equals(id))
                        continue outer;

                data_colnames[j] = colname;
                j++;
            }


            //separating id_columns into separate arrays- not dataframe
            for(int i = 0; i<colnames.length; i++) {
                Column temp = groups.getFirst().get(colnames[i]);
                id_columns[i] = new Column(temp.nazwa, temp.typ);

            }

            try {

                for(DataFrame df : groups) {
                    for (int i = 0; i < id_colnames.length; i++) {

                        id_columns[i].add(df.get(id_colnames[i]).get(0));

                    }
                }

            } catch (DFColumnTypeException e) {
                e.printStackTrace();
            }


        }

        public LinkedList<DataFrame> getGroups() {
            return groups;
        }

        @Override
        public DataFrame apply(Applyable function) throws DFApplyableException {

            try {
                DataFrame output =null;
                for (int group = 0; group < groups.size(); group++) {


                    DataFrame temp = function.apply(groups.get(group).get(data_colnames,false));
                    //inicjalizacja DataFrame output
                    //tak żeby zawierał otpowiednie typy kolunm na wyjściu
                    if(output == null){
                        String[]   temp_colnames = temp.getNames();
                        String[] output_colnames = new String[temp_colnames.length+id_colnames.length];
                        Class<? extends Value>[]   temp_types = temp.getTypes();
                        //noinspection unchecked
                        Class<? extends Value>[] output_types = new Class[temp_colnames.length+id_colnames.length];

                        for (int i = 0; i < output_colnames.length; i++) {
                            output_colnames[i]= (i<id_colnames.length) ?
                                    id_colnames[i] :
                                    temp_colnames[i-id_colnames.length];

                            output_types[i]=(i<id_colnames.length) ?
                                    id_columns[i].getType():
                                    temp_types[i-id_colnames.length];
                        }

                        output = new DataFrame(output_colnames,output_types);
                    }


                    //przepisanie wartości z temp, jeżelicoś zawiera
                    if(temp.size()>0) {

                        Value[] output_row = new Value[output.getColCount()];
                        //wpisanie identyfykatora wiersza
                        for (int i = 0; i < id_columns.length; i++) {
                            output_row[i]=id_columns[i].get(group);
                        }

                        //przepisanie wartości z temp
                        for (int i = 0; i < temp.size(); i++) {
                            Value[] temp_row=temp.getRecord(i);
                            if (output.getColCount() - id_columns.length >= 0)
                                System.arraycopy(temp_row, 0, output_row, id_columns.length, output.getColCount() - id_columns.length);

//                        for(int j=id_columns.length;j<output.getColCount();j++)
//                            output_row[j]=temp_row[j-id_columns.length];
                            output.addRecord(output_row);
                        }
                    }

                }


                return output;

            } catch (DFColumnTypeException | CloneNotSupportedException e) {
                throw new DFApplyableException(e.getMessage());
            }

        }



}


}
