package lab0.dataframe;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.NoSuchElementException;

public class DataFrame {
    /**
     * Data container
    */
    public class Kolumna{
        ArrayList dane;
        String nazwa;
        DataType typ;

        /**
         *
         * @param nazwa Kolumny
         * @param typ Przechowywany typ danych
         */
        Kolumna(String nazwa,DataType typ){
            dane = new ArrayList();
            this.nazwa=nazwa;
            this.typ=typ;
        }

        /**
         * Kopiowanie
         * @param source klumna do skopiowania
         */
        Kolumna(Kolumna source){

            this.nazwa=source.nazwa;
            this.typ=source.typ;
            dane = new ArrayList();
            for(Object o:source.dane){
                add(typ.cloneData(o));
            }
        }



        /**
         * Accesor
         * @param index wiersz
         * @return Obiekt w wierzu I
         */
        public Object get(int index){
            return dane.get(index);
        }

        /**
         *
         * @param o nowy wiersz
         */
        void add(Object o){
            if(typ.isCorrectType(o))
                dane.add(o);
            else
                throw new DFException("this shouldn't happen - illegal type");
        }

        /**
         *
         * @return ilość elementyów
         */
        public int size(){
            return dane.size();
        }

        @Override
        public String toString() {

            StringBuilder s=new StringBuilder();
            s.append(nazwa+" : "+typ+'\n');
            for(Object o:dane)
                s.append(o.toString()+'\n');
            return s.toString();
        }

        public Kolumna copy(){
            return new Kolumna(this);
        }
    }
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////
    protected Kolumna[] kolumny;
    protected int rowNumber;

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////
    protected void readFile(String path, boolean header) throws IOException{
        FileInputStream fstream = new FileInputStream(path);
        BufferedReader br = new BufferedReader(new InputStreamReader(fstream));

        String[] strLine;

        if(header)
        {
            strLine=br.readLine().split(",");
            for (int i = 0; i < kolumny.length; i++) {
                kolumny[i].nazwa=strLine[i];
            }
        }

        String temp;
        Object[] tempValues=new Object[kolumny.length];
        while((temp=br.readLine())!=null)
        {
            strLine=temp.split(",");
            int i=0;
            for(String s:strLine) {
                tempValues[i] = DataType.fromString(kolumny[i].typ, s);
                i++;
            }
            addRecord(tempValues);
        }

        br.close();

    }

    //true
    public DataFrame(String path,String[] typykolumn) throws IOException{
        this(path,typykolumn,null);
    }

    //false
    public DataFrame(String path,String[] typykolumn,String[] nazwykolumn) throws IOException{
        this(typykolumn.length);
        boolean header = nazwykolumn==null;
        for(int i=0;i<typykolumn.length;i++)
            kolumny[i]=new Kolumna(header? "" : nazwykolumn[i],DataType.getDataType(typykolumn[i]));
        readFile(path,header);
    }



    protected DataFrame(int count){
        kolumny=new Kolumna[count];
        rowNumber=0;
    }
    public DataFrame(String[] nazwyKolumn,DataType[] typyKolumn){
        this(nazwyKolumn.length);
        construct(nazwyKolumn,typyKolumn);
    }
    public DataFrame(String[] nazwyKolumn,String[] typyKolumn){
        this(nazwyKolumn.length);
        DataType[] types=new DataType[typyKolumn.length];
        for(int i =0; i<typyKolumn.length;i++)
            types[i]=DataType.getDataType(typyKolumn[i]);
        construct(nazwyKolumn,types);
    }
    private void construct(String[] nazwyKolumn,DataType[] typyKolumn){
        for(int i =0; i<typyKolumn.length;i++)
            kolumny[i]=new Kolumna(nazwyKolumn[i],typyKolumn[i]);
    }


    public DataFrame(Kolumna[] kolumny){
        this.kolumny=kolumny;
        rowNumber=kolumny[0].size();
        for(Kolumna k:kolumny)
            if(k.size()!=rowNumber)
                throw new DFException("this shouldn't happen");
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

    public DataType[] getTypes(){
        DataType[] types= new DataType[kolumny.length];
        for(int i=0;i<kolumny.length;i++)
            types[i]=kolumny[i].typ;
        return types;
    }

    /**
     * getter kolumny o danej nazwie
     * zwraca pierwsza kolumnę o danej nazwie
     * @param colname
     * @return kolumna
     */
    public Kolumna get(String colname){
        for (Kolumna k:kolumny)
            if(k.nazwa.equals(colname))
                return  k;

        throw new NoSuchElementException("No such column: "+colname);
    }

    //todo: col name check
    //todo: more exceptions - categorised
    /**
     * Get DataFrame o danych kolumnach
     * @param cols nazwy kolumn
     * @param copy wykonanie głębokiej kopii
     * @return podzbiór dF
     */
    public DataFrame get(String[] cols,boolean copy){
        Kolumna[] kolumny = new Kolumna[cols.length];

        for(int i=0;i<cols.length;i++)
            if(copy)
                kolumny[i]=get(cols[i]).copy();
            else
                kolumny[i]=get(cols[i]);

        return new DataFrame(kolumny);
    }



    /**
     * Dadanie rekordu do Dataframe
     * @param vals elementy rekordu
     */
    public void addRecord(Object... vals) {
        if(vals.length!=kolumny.length)
            throw new DFException("This shoudn't happen, but i can see why could");
        for(int i=0;i<kolumny.length;i++)
            if(!kolumny[i].typ.isCorrectType(vals[i]))
                throw new DFException("This shoudn't happen, but i can see why could");

        int i=0;
        rowNumber++;
        for(Kolumna k:kolumny)
            k.add(vals[i++]);

    }

    /**
     * Zwraca wiersz jako Array obiektów
     * @param i nr.wiersz
     * @return wiersz
     */
    public Object[] getRecord(int i){
        Object[] temp=new Object[kolumny.length];
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
    public DataFrame iloc(int i){

        return iloc(i,i);
    }

    /**
     * Zwraca wiersze jako DataFrame
     * @param from od
     * @param to  do
     * @return Wiersze
     */
    public DataFrame iloc(int from ,int to){
        if(from<0 || from>=rowNumber)
            throw new DFException("No such index: "+from);

        if(to<0 || to>=rowNumber)
            throw new DFException("No such index: "+to);

        if(to<from)
            throw new DFException("unable to create range from "+from+" to "+to);

        String[] nazwy = new String[kolumny.length];
        DataType[] typy = new DataType[kolumny.length];
        for(int i=0;i<kolumny.length;i++) {
            nazwy[i] =kolumny[i].nazwa;
            typy[i] = kolumny[i].typ;
        }

        DataFrame df=new DataFrame(nazwy,typy);
        Object[] temp=new Object[kolumny.length];

        for(int i=from;i<=to;i++){
            df.addRecord(getRecord(i));
        }

        return df;
    }

    @Override
    public String toString() {
        StringBuilder s=new StringBuilder();
        for(Kolumna k:kolumny)
            s.append("|"+k.nazwa+":"+k.typ.id);
        s.append("|\n");
        for(int i=0;i<rowNumber;i++){
            for(Kolumna k:kolumny)
                s.append("|"+k.get(i).toString());
            s.append("|\n");
        }
        return s.toString();
    }


}
