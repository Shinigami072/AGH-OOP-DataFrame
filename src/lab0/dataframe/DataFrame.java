package lab0.dataframe;

import lab0.dataframe.exceptions.*;
import lab0.dataframe.groupby.Applyable;
import lab0.dataframe.groupby.GroupBy;
import lab0.dataframe.values.Value;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class DataFrame {
    //true
    public DataFrame(String path, Class<? extends Value>[] typy_kolumn) throws IOException, DFColumnTypeException {
        this(path, typy_kolumn, null);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////
    protected Kolumna[] kolumny;
    protected int rowNumber;

    //false
    public DataFrame(String path, Class<? extends Value>[] typy_kolumn, String[] nazwy_kolumn) throws IOException, DFColumnTypeException {
        this(typy_kolumn.length);
        boolean header = nazwy_kolumn == null;
        for (int i = 0; i < typy_kolumn.length; i++)
            kolumny[i] = new Kolumna(header ? "" : nazwy_kolumn[i], typy_kolumn[i]);
        readFile(path, header);
    }

    protected DataFrame(int count) throws DFZeroLengthCreationException {
        if (count == 0)
            throw new DFZeroLengthCreationException();
        kolumny = new Kolumna[count];
        rowNumber = 0;

    }

    public DataFrame(Kolumna[] kolumny) throws DFZeroLengthCreationException {
        if (kolumny.length == 0)
            throw new DFZeroLengthCreationException();
        this.kolumny = kolumny;
        rowNumber = kolumny[0].size();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////
    protected void readFile(String path, boolean header) throws IOException, DFColumnTypeException {
        FileInputStream fileStream = new FileInputStream(path);

        try (BufferedReader br = new BufferedReader(new InputStreamReader(fileStream))) {
            String[] strLine;
            if (header) {
                strLine = br.readLine().split(",");
                for (int i = 0; i < kolumny.length; i++) {
                    kolumny[i].nazwa = strLine[i];
                }
            }

            String temp;
            Value[] tempValues = new Value[kolumny.length];
            Value.ValueBuilder[] builders = new Value.ValueBuilder[kolumny.length];
            for (int i = 0; i < kolumny.length; i++) {
                builders[i] = Value.builder(kolumny[i].typ);
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
    public DataFrame(String[] nazwyKolumn,Class<? extends Value>[] typyKolumn){
        this(nazwyKolumn.length);
        for(int i =0; i<typyKolumn.length;i++)
            kolumny[i]=new Kolumna(nazwyKolumn[i],typyKolumn[i]);
    }

    /**
     * Get DataFrame o danych kolumnach
     *
     * @param cols nazwy kolumn
     * @param copy wykonanie głębokiej kopii
     * @return podzbiór dF
     */
    public DataFrame get(String[] cols, boolean copy) throws DFZeroLengthCreationException, CloneNotSupportedException {
        Kolumna[] kolumny = new Kolumna[cols.length];

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
        String[] names = new String[kolumny.length];
        for(int i=0;i<kolumny.length;i++)
            names[i]=kolumny[i].nazwa;
        return names;
    }

    public Class<? extends Value>[] getTypes(){
        Class<? extends Value>[] types= ( Class<? extends Value>[])(new Class[kolumny.length]);
        for(int i=0;i<kolumny.length;i++)
            types[i]=kolumny[i].typ;
        return types;
    }

    /**
     * getter kolumny o danej nazwie
     * zwraca pierwsza kolumnę o danej nazwie
     * @param colname nazwa kolumny
     * @return kolumna
     */
    public Kolumna get(String colname){
        for (Kolumna k:kolumny)
            if(k.nazwa.equals(colname))
                return  k;

        throw new NoSuchElementException("No such column: "+colname);
    }

    /**
     * Dodanie rekordu do DataFrame
     * @param values elementy rekordu
     */
    public void addRecord(Value... values) throws DFColumnTypeException, DFDimensionException {
        if(values.length!=kolumny.length)
            throw new DFDimensionException(String.format("DF col %d , record length: %d", colCount(), values.length));
        for(int i = 0; i<kolumny.length; i++)
            if(!kolumny[i].typ.isInstance(values[i]))
                throw new DFColumnTypeException(kolumny[i], values[i],i);

        int i=0;
        rowNumber++;
        for(Kolumna k:kolumny)
            k.add(values[i++]);

    }

    public int colCount() {
        return kolumny.length;
    }

    /**
     * Zwraca wiersz jako Array obiektów
     * @param i nr.wiersz
     * @return wiersz
     */
    public Value[] getRecord(int i) {
        if (i < 0 || i > size())
            throw new DFIndexOutOfBounds("Out of bounds: "+i);
        Value[] temp=new Value[kolumny.length];
        int j=0;
        for(Kolumna k:kolumny)
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
        if(from<0 || from>=rowNumber)
            throw new DFIndexOutOfBounds("No such index: "+from);

        if(to<0 || to>=rowNumber)
            throw new DFIndexOutOfBounds("No such index: "+to);

        if(to < from)
            throw new DFIndexOutOfBounds("unable to create range from " + from + " to "+to);

        String[] nazwy = new String[kolumny.length];
        Class<? extends Value>[] typy= ( Class<? extends Value>[])(new Class[kolumny.length]);
        for(int i=0;i<kolumny.length;i++) {
            nazwy[i] =kolumny[i].nazwa;
            typy[i] = kolumny[i].typ;
        }

        DataFrame df=new DataFrame(nazwy,typy);

        for(int i=from;i<=to;i++){
            df.addRecord(getRecord(i));
        }

        return df;
    }

    public Grupator4000 groupBy(String... colname) throws CloneNotSupportedException {

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

//    public Hashtable<Value, DataFrame> groupBy(String colname){
//        Kolumna kol  =get(colname);
//        Hashtable<Value, DataFrame> output = new Hashtable<>(kol.uniqueSize());
//
//        for (int i = 0; i < rowNumber; i++) {
//            Value key=kol.get(i);
//            Value[] row = getRecord(i);
//            DataFrame ll = output.get(key);
//
//            if(ll!= null)
//               ll.addRecord(row);
//
//            else{
//                ll  =new SparseDataFrame(getNames(),getTypes(),row);
//                ll.addRecord(row);
//                output.put(key,ll);
//            }
//        }
//        return output;
//    }
//
//    public Grupator4000 groupBy(String[] colname){
//        Hashtable<Value,DataFrame> initial =  groupBy(colname[0]);
//
//        LinkedList<DataFrame> lista;
//
//        if(colname.length ==1)
//            lista = new LinkedList<>(initial.values());
//        else
//            lista= new LinkedList<>();
//
//        LinkedList<DataFrame> temp= new LinkedList<>(initial.values());
//
//        for (int i = 1; i < colname.length; i++) {
//            lista.clear();
//            for(DataFrame df:temp)
//                lista.addAll(df.groupBy(colname[i]).values());
//            temp.clear();
//            temp.addAll(lista);
//        }
//
//        return new Grupator4000(lista,colname);
//
//    }

    /**
     * Data container
     */
    public static class Kolumna {
        final ArrayList<Value> dane;

        /**
         *
         * @param nazwa Kolumny
         * @param typ Przechowywany typ danych
         */
        public Kolumna(String nazwa, Class<? extends Value> typ) {
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
        Kolumna(Kolumna source) throws CloneNotSupportedException {

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

        public String getNazwa() {
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

        public Kolumna copy() throws CloneNotSupportedException {
            return new Kolumna(this);
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

        protected Kolumna performOperation(Value.OPERATION_TYPES operation, Value operand) throws DFColumnTypeException {
            Kolumna output = new Kolumna(getNazwa(), (get(0).operate(operation, operand)).getClass());

            for (Value v : dane) {
                output.add(v.operate(operation, operand));
            }

            return output;
        }

        protected Kolumna performOperation(Value.OPERATION_TYPES operation, Kolumna operand) throws DFColumnTypeException {
            if (operand.size() != size())
                throw new DFDimensionException(
                        String.format("kolumn %s (%d) , kolumn %s (%d) have different length",
                                getNazwa(), size(),
                                operand.getNazwa(), operand.size()));

            Kolumna output = new Kolumna(getNazwa(), get(0).operate(operation, operand.get(0)).getClass());

            for (int i = 0; i < size(); i++) {
                output.add(get(i).operate(operation, operand.get(i)));
            }


            return output;
        }

        public Kolumna v_add(Value v) throws DFColumnTypeException {
            return performOperation(Value.OPERATION_TYPES.ADD, v);
        }

        public Kolumna v_sub(Value v) throws DFColumnTypeException {
            return performOperation(Value.OPERATION_TYPES.SUB, v);
        }

        public Kolumna v_mul(Value v) throws DFColumnTypeException {
            return performOperation(Value.OPERATION_TYPES.MUL, v);
        }

        public Kolumna v_div(Value v) throws DFColumnTypeException {
            return performOperation(Value.OPERATION_TYPES.DIV, v);
        }

        public Kolumna v_pow(Value v) throws DFColumnTypeException {
            return performOperation(Value.OPERATION_TYPES.POW, v);
        }

        public Kolumna v_add(Kolumna k) throws DFColumnTypeException {
            return performOperation(Value.OPERATION_TYPES.ADD, k);
        }

        public Kolumna v_sub(Kolumna k) throws DFColumnTypeException {
            return performOperation(Value.OPERATION_TYPES.SUB, k);
        }

        public Kolumna v_mul(Kolumna k) throws DFColumnTypeException {
            return performOperation(Value.OPERATION_TYPES.MUL, k);
        }

        public Kolumna v_div(Kolumna k) throws DFColumnTypeException {
            return performOperation(Value.OPERATION_TYPES.DIV, k);
        }

        public Kolumna v_pow(Kolumna k) throws DFColumnTypeException {
            return performOperation(Value.OPERATION_TYPES.POW, k);
        }


        @Override
        public boolean equals(Object obj) {
            if (obj instanceof Kolumna) {
                Kolumna k = (Kolumna) obj;

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

    public final class Grupator4000 implements GroupBy {

        private LinkedList<DataFrame> groups;
        private String[]   id_colnames;
        private String[] data_colnames;

        private Kolumna[] id_columns;


        public Grupator4000(Collection<DataFrame> collection, String[] colnames) {
            groups = new LinkedList<>(collection);
            id_colnames=colnames;
            id_columns= new Kolumna[colnames.length];

            String[] all_colnames = groups.getFirst().getNames();
            data_colnames= new String[all_colnames.length-id_colnames.length];

            int j =0;
            outer:
            for (String colname : all_colnames) {

                for (String id : id_colnames)
                    if (colname.equals(id))
                        continue outer;

                data_colnames[j] = colname;
                j++;
            }

            for(int i = 0; i<colnames.length; i++) {
                Kolumna temp = groups.getFirst().get(colnames[i]);
                id_columns[i] = new Kolumna(temp.nazwa,temp.typ);

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

                        Value[] output_row = new Value[output.colCount()];
                        //wpisanie identyfykatora wiersza
                        for (int i = 0; i < id_columns.length; i++) {
                            output_row[i]=id_columns[i].get(group);
                        }

                        //przepisanie wartości z temp
                        for (int i = 0; i < temp.size(); i++) {
                            Value[] temp_row=temp.getRecord(i);
                            if (output.colCount() - id_columns.length >= 0)
                                System.arraycopy(temp_row, 0, output_row, id_columns.length, output.colCount() - id_columns.length);

//                        for(int j=id_columns.length;j<output.colCount();j++)
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

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof DataFrame)
        {
            DataFrame df = (DataFrame)obj;
            return Arrays.deepEquals(kolumny,df.kolumny);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(kolumny);
    }

    @Override
    public String toString() {
        StringBuilder s=new StringBuilder();
        String[] str;
        for(Kolumna k:kolumny){
            str=k.typ.getTypeName().split("\\.");
            s.append(String.format("|%-14.14s:%15.15s",k.nazwa ,str[str.length-1]));
        }
        s.append("|\n");
        for(int i=0;i<rowNumber;i++){
            for(Kolumna k:kolumny)
                s.append(String.format("|%30.30s",k.get(i).toString()));
            s.append("|\n");
        }
        return s.toString();
    }


}
