package lab0.dataframe;

import java.io.IOException;

public class SparseDataFrame extends DataFrame {

    final class COOValue{

        private final Object value;
        private final int index;
        COOValue(int index,Object value){
            this.index=index;
            this.value=value;
        }
        public int getIndex() {
            return index;
        }

        public Object getValue(){
            return value;
        }

        @Override
        public String toString() {
            return "COOValue(" + index + ", " + value.toString() + ')';
        }

    }
    public class SparseKolumna extends Kolumna{

        final Object hidden;

        int size;
        /**
         *
         * @param nazwa Kolumny
         * @param typ Przechowywany typ danych
         * @param hidden Przechowywany obiekt
         */
        SparseKolumna(String nazwa, DataType typ,Object hidden) {
            super(nazwa, typ);
            this.hidden=hidden;
            size=0;
        }
        /**
         * Kopiowanie
         * @param source kolumna do skopiowania
         */
        public SparseKolumna(SparseKolumna source) {
            super(source.nazwa,source.typ);
            this.hidden = source.hidden;
            size = source.size;
            for(int i=0;i<source.dane.size();i++)
            {
                COOValue v =((COOValue)source.dane.get(i));
                dane.add(new COOValue(v.index,typ.cloneData(v.value)));
            }
        }
        /**
         *
         * @param o nowy wiersz
         */
        @Override
        void add(Object o) {
            if(typ.isCorrectType(o)){
                if(!hidden.equals(o))
                    dane.add(new COOValue(size,o));
                size++;
            }
            else
                throw new DFException("this shouldn't happen - illegal type");
        }

        /**
         * Accessor
         * @param index wiersz
         * @return Obiekt w wierszu I
         */
        @Override
        public Object get(int index) {

           //binary search
            int iR=dane.size()-1;
            int iL=0;
            int t,i;
            if(dane.size()==0 && size>0)
                return hidden;
            do{
                i = Math.floorDiv((iR+iL),2);
                t=((COOValue)dane.get(i)).getIndex();

                if(t<index)
                    iL=i+1;
                else if(t>index)
                    iR=i-1;
                else
                    return ((COOValue)dane.get(i)).getValue();

            }while(iL<=iR);

            return hidden;
        }

        /**
         *
         * @return ilość elementów
         */
        @Override
        public int size() {
            return size;
        }

        /**
         *
         * @return ilość elementów
         */
        @Override
        public Kolumna copy() {
            return new SparseKolumna(this);
        }

    }
    //true
    public SparseDataFrame(String path,String[] typy_kolumn,Object[] hide) throws IOException {
        this(path,typy_kolumn,hide,null);
    }
    //todo: unitTesty

    //false
    public SparseDataFrame(String path,String[] typy_kolumn,Object[] hide,String[] nazwy_kolumn) throws IOException{
        super(typy_kolumn.length);
        boolean header = nazwy_kolumn==null;
        for(int i=0;i<typy_kolumn.length;i++)
            kolumny[i]=new SparseKolumna(header? "" : nazwy_kolumn[i],DataType.getDataType(typy_kolumn[i]),hide[i]);
        readFile(path,header);
    }
    public SparseDataFrame(String[] nazwyKolumn, DataType[] typyKolumn, Object[] hide) {
        super(nazwyKolumn.length);
        for(int i =0; i<typyKolumn.length;i++)
            kolumny[i]=new SparseKolumna(nazwyKolumn[i],typyKolumn[i],hide[i]);
    }

    public SparseDataFrame(String[] nazwyKolumn, String[] typyKolumn, Object[] hide) {
        super(nazwyKolumn.length);
        for(int i =0; i<typyKolumn.length;i++)
            kolumny[i]=new SparseKolumna(nazwyKolumn[i],DataType.getDataType(typyKolumn[i]),hide[i]);
    }
    public SparseDataFrame(DataFrame df,Object[] hide){
        super(df.kolumny.length);
        String[] nazwyKolumn=df.getNames();
        DataType[] typyKolumn=df.getTypes();
        for(int i =0; i<typyKolumn.length;i++)
            kolumny[i]=new SparseKolumna(nazwyKolumn[i],typyKolumn[i],hide[i]);

        Object[] temp = new Object[df.kolumny.length];
        for(int i=0;i<df.rowNumber;i++) {
            int j=0;
            for(Kolumna k:df.kolumny)
                temp[j++]=k.get(i);
            addRecord(temp);
        }
    }

    public SparseDataFrame(SparseKolumna[] kolumny) {
        super(kolumny);
    }


    /**
     * getter kolumny o danej nazwie
     * zwraca pierwsza kolumnę o danej nazwie
     * @param colname nazwa kolumny
     * @return kolumna
     */
    @Override
    public SparseKolumna get(String colname) {
        return (SparseKolumna) super.get(colname);
    }

    /**
     * Get SparseDataFrame o danych kolumnach
     * @param cols nazwy kolumn
     * @param copy wykonanie głębokiej kopii
     * @return podzbiór dF
     */
    @Override
    public SparseDataFrame get(String[] cols, boolean copy) {
        SparseKolumna[] kolumny = new SparseKolumna[cols.length];

        for(int i=0;i<cols.length;i++)
            if(copy)
                kolumny[i]=(SparseKolumna) get(cols[i]).copy();
            else
                kolumny[i]= get(cols[i]);

        return new SparseDataFrame(kolumny);
    }

    /**
     * Zwraca wiersz jako SparseDataFrame
     * @param i nr wiersza
     * @return Wiersz
     */
    @Override
    public SparseDataFrame iloc(int i) {
        return iloc(i,i);
    }

    /**
     * Zwraca wiersze jako SparseDataFrame
     * @param from od
     * @param to  do
     * @return Wiersze
     */
    @Override
    public SparseDataFrame iloc(int from, int to) {
        if(from<0 || from>=rowNumber)
            throw new DFException("No such index: "+from);

        if(to<0 || to>=rowNumber)
            throw new DFException("No such index: "+to);

        if(to<from)
            throw new DFException("unable to create range from "+from+" to "+to);

        String[] nazwy = new String[kolumny.length];
        DataType[] typy = new DataType[kolumny.length];
        Object[] hidden= new Object[kolumny.length];
        for(int i=0;i<kolumny.length;i++) {
            nazwy[i] =kolumny[i].nazwa;
            typy[i] = kolumny[i].typ;
            hidden[i] = ((SparseKolumna)kolumny[i]).hidden;
        }

        SparseDataFrame df=new SparseDataFrame(nazwy,typy,hidden);

        for(int i=from;i<=to;i++){
            df.addRecord(getRecord(i));
        }

        return df;
    }

    /**
     * Zwraca DF wypełniony normalnieS
     * @return normalny DataFrame
     */
    public DataFrame toDense(){
        String[] nazwy = new String[kolumny.length];
        DataType[] typy = new DataType[kolumny.length];
        for(int i=0;i<kolumny.length;i++) {
            nazwy[i] =kolumny[i].nazwa;
            typy[i] = kolumny[i].typ;
        }

        Object[] temp =new Object[typy.length];
        DataFrame df = new DataFrame(nazwy,typy);
        for(int i=0;i<size();i++)
        {
            Object[] row =getRecord(i);
            for(int j=0;j<typy.length;j++)
                temp[j]=typy[j].cloneData(row[j]);
            df.addRecord(temp);
        }
        return df;
    }

}

