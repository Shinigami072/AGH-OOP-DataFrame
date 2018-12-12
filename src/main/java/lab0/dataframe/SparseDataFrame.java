package lab0.dataframe;

import lab0.dataframe.exceptions.DFColumnTypeException;
import lab0.dataframe.exceptions.DFDimensionException;
import lab0.dataframe.exceptions.DFIndexOutOfBounds;
import lab0.dataframe.exceptions.DFValueBuildException;
import lab0.dataframe.values.Value;

import java.io.IOException;

public class SparseDataFrame extends DataFrame {

    //true
    public SparseDataFrame(String path, Value[] hide) throws IOException, DFColumnTypeException, DFValueBuildException {
        this(path, hide, null);
    }

    //false
    public SparseDataFrame(String path, Value[] hide, String[] nazwy_kolumn) throws IOException, DFColumnTypeException, DFValueBuildException {
        super(hide.length);
        boolean header = nazwy_kolumn == null;
        for (int i = 0; i < hide.length; i++)
            kolumny[i] = new SparseColumn(header ? "" : nazwy_kolumn[i], hide[i]);
        readFile(path, header);
    }

    public SparseDataFrame(String[] nazwyKolumn, Value[] hide) {
        super(nazwyKolumn.length);
        for (int i = 0; i < hide.length; i++)
            kolumny[i] = new SparseColumn(nazwyKolumn[i], hide[i]);
    }

    public SparseDataFrame(DataFrame df, Value[] hide) {
        super(df.kolumny.length);
        String[] nazwyKolumn = df.getNames();
        for (int i = 0; i < hide.length; i++)
            kolumny[i] = new SparseColumn(nazwyKolumn[i], hide[i]);

        try {
            Value[] temp = new Value[df.kolumny.length];
            for (int i = 0; i < df.rowNumber; i++) {
                int j = 0;
                for (Column k : df.kolumny)
                    temp[j++] = k.get(i);
                addRecord(temp);
            }

        } catch (DFColumnTypeException e) {
            e.printStackTrace();
        }
    }

    public SparseDataFrame(SparseColumn[] kolumny) {
        super(kolumny);
    }

    /**
     * getter kolumny o danej nazwie
     * zwraca pierwsza kolumnę o danej nazwie
     *
     * @param colname nazwa kolumny
     * @return kolumna
     */
    @Override
    public SparseColumn get(String colname) {
        return (SparseColumn) super.get(colname);
    }

    /**
     * Get SparseDataFrame o danych kolumnach
     *
     * @param cols nazwy kolumn
     * @param copy wykonanie głębokiej kopii
     * @return podzbiór dF
     */
    @Override
    public SparseDataFrame get(String[] cols, boolean copy) {
        SparseColumn[] kolumny = new SparseColumn[cols.length];

        for (int i = 0; i < cols.length; i++)
            if (copy)
                kolumny[i] = get(cols[i]).copy();
            else
                kolumny[i] = get(cols[i]);

        return new SparseDataFrame(kolumny);
    }

    /**
     * Zwraca wiersz jako SparseDataFrame
     *
     * @param i nr wiersza
     * @return Wiersz
     */
    @Override
    public SparseDataFrame iloc(int i) {
        return iloc(i, i);
    }

    /**
     * Zwraca wiersze jako SparseDataFrame
     *
     * @param from od
     * @param to   do
     * @return Wiersze
     */
    @Override
    public SparseDataFrame iloc(int from, int to) {
        if (from < 0 || from >= rowNumber)
            throw new DFIndexOutOfBounds("No such index: " + from);

        if (to < 0 || to >= rowNumber)
            throw new DFIndexOutOfBounds("No such index: " + to);

        if (to < from)
            throw new DFIndexOutOfBounds("unable to create range from " + from + " to " + to);


        String[] nazwy = new String[kolumny.length];
        Class<? extends Value>[] typy = (Class<? extends Value>[]) new Class[kolumny.length];
        Value[] hidden = new Value[kolumny.length];
        for (int i = 0; i < kolumny.length; i++) {
            nazwy[i] = kolumny[i].nazwa;
            typy[i] = kolumny[i].typ;
            hidden[i] = ((SparseColumn) kolumny[i]).hidden;
        }

        SparseDataFrame df = new SparseDataFrame(nazwy, hidden);
        try {

            for (int i = from; i <= to; i++) {
                df.addRecord(getRecord(i));
            }

        } catch (DFColumnTypeException e) {
            e.printStackTrace();
        }

        return df;
    }

    /**
     * Zwraca DF wypełniony normalnieS
     *
     * @return normalny DataFrame
     */
    public DataFrame toDense() {
        String[] nazwy = new String[kolumny.length];
        Class<? extends Value>[] typy = (Class<? extends Value>[]) new Class[kolumny.length];
        for (int i = 0; i < kolumny.length; i++) {
            nazwy[i] = kolumny[i].nazwa;
            typy[i] = kolumny[i].typ;
        }

        Value[] temp = new Value[typy.length];
        DataFrame df = new DataFrame(nazwy, typy);
        try {

            for (int i = 0; i < size(); i++) {
                Value[] row = getRecord(i);
                for (int j = 0; j < typy.length; j++) {
                    try {
                        temp[j] = (row[j]).clone();
                    } catch (CloneNotSupportedException e) {
                        temp[j] = (row[j]);
                    }
                }

                df.addRecord(temp);

            }

        } catch (DFColumnTypeException e) {
            e.printStackTrace();
        }
        return df;
    }

    final static class COOValue extends Value {

        private final Value value;
        private final int index;

        COOValue(int index, Value value) {
            this.index = index;
            this.value = value;
        }

        public int getIndex() {
            return index;
        }

        public Value getValue() {
            return value;
        }

        @Override
        public String toString() {
            return "COOValue(" + index + ", " + value.toString() + ')';
        }

        @Override
        public Value create(String s) {
            return value.create(s);
        }

        @Override
        public Value add(Value v) {
            if (v instanceof COOValue)
                return value.add(((COOValue) v).getValue());
            else
                return value.add(v);
        }

        @Override
        public Value sub(Value v) {
            if (v instanceof COOValue)
                return value.sub(((COOValue) v).getValue());
            else
                return value.sub(v);
        }

        @Override
        public Value mul(Value v) {
            if (v instanceof COOValue)
                return value.mul(((COOValue) v).getValue());
            else
                return value.mul(v);
        }

        @Override
        public Value div(Value v) {
            if (v instanceof COOValue)
                return value.div(((COOValue) v).getValue());
            else
                return value.div(v);
        }

        @Override
        public Value pow(Value v) {
            if (v instanceof COOValue)
                return value.pow(((COOValue) v).getValue());
            else
                return value.pow(v);
        }

        @Override
        public boolean eq(Value v) {
            if (v instanceof COOValue)
                return value.eq(((COOValue) v).getValue());
            else
                return value.eq(v);
        }

        @Override
        public boolean lte(Value v) {
            if (v instanceof COOValue)
                return value.lte(((COOValue) v).getValue());
            else
                return value.lte(v);
        }

        @Override
        public boolean gte(Value v) {
            if (v instanceof COOValue)
                return value.gte(((COOValue) v).getValue());
            else
                return value.gte(v);
        }

        @Override
        public boolean neq(Value v) {
            if (v instanceof COOValue)
                return value.neq(((COOValue) v).getValue());
            else
                return value.neq(v);
        }

    }

    public static class SparseColumn extends Column {

        final Value hidden;
        int size;

        /**
         * @param nazwa  Kolumny
         * @param hidden Przechowywany obiekt
         */
        SparseColumn(String nazwa, Value hidden) {
            super(nazwa, hidden.getClass());
            this.hidden = hidden;
            size = 0;
        }

        /**
         * Kopiowanie
         *
         * @param source kolumna do skopiowania
         */
        public SparseColumn(SparseColumn source) {
            super(source.nazwa, source.typ);
            this.hidden = source.hidden;
            size = source.size;
            for (int i = 0; i < source.dane.size(); i++) {
                COOValue v = (COOValue) (source.dane.get(i));
                try {
                    dane.add(new COOValue(v.index, v.value.clone()));
                } catch (CloneNotSupportedException e) {
                    dane.add(new COOValue(v.index, v.value));
                }
            }
        }

        /**
         * @param o nowy wiersz
         */
        @Override
        public void add(Value o) throws DFColumnTypeException {
            if (typ.isInstance(o)) {
                if (!hidden.equals(o))
                    dane.add(new COOValue(size, o));
                size++;
            } else
                throw new DFColumnTypeException(this, o, size);
        }

        /**
         * Accessor
         *
         * @param index wiersz
         * @return Obiekt w wierszu I
         */
        @Override
        public Value get(int index) {

            if (index < 0 || index >= size)
                throw new DFIndexOutOfBounds("index: " + index);
            //binary search
            int iR = dane.size() - 1;
            int iL = 0;
            int t, i;
            if (dane.size() == 0)
                return hidden;
            do {
                i = Math.floorDiv((iR + iL), 2);
                t = ((COOValue) (dane.get(i))).getIndex();

                if (t < index)
                    iL = i + 1;
                else if (t > index)
                    iR = i - 1;
                else
                    return ((COOValue) (dane.get(i))).getValue();

            } while (iL <= iR);

            return hidden;
        }

        @Override
        protected Column performOperation(Value.OPERATION_TYPES operation, Value operand) {
            Column output = new SparseColumn(getName(), hidden.operate(operation, operand));
            try {
                for (Value v : dane) {
                    output.add(v.operate(operation, operand));
                }

            } catch (DFColumnTypeException e) {
                e.printStackTrace();
            }
            return output;
        }

        @Override
        protected Column performOperation(Value.OPERATION_TYPES operation, Column operand) {
            if (operand.size() != size())
                throw new DFDimensionException(
                        String.format("kolumn %s (%d) , kolumn %s (%d) have different length",
                                getName(), size(),
                                operand.getName(), operand.size()));

            SparseColumn output = new SparseColumn(getName(), hidden.operate(operation, ((SparseColumn) operand).hidden));
            try {

                for (int i = 0; i < size(); i++) {
                    if (get(i).equals(hidden))
                        output.add(output.hidden);
                    else
                        output.add(get(i).operate(operation, operand.get(i)));
                }
            } catch (DFColumnTypeException e) {
                e.printStackTrace();
            }

            return output;

        }


        /**
         * @return ilość elementów
         */
        @Override
        public int size() {
            return size;
        }

        @Override
        public int uniqueSize() {
            return dane.size();
        }

        /**
         * @return kopia kolumny
         */
        @Override
        public SparseColumn copy() {
            return new SparseColumn(this);
        }

    }

}

