package lab0.dataframe;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.NoSuchElementException;

public class DataFrame {
    public class Kolumna{
        ArrayList dane;
        String nazwa;
        DataType typ;
        Kolumna(String nazwa,DataType typ){
            dane = new ArrayList();
            this.nazwa=nazwa;
            this.typ=typ;
        }

        Kolumna(Kolumna source){
            dane = new ArrayList(source.dane);
            this.nazwa=new String(source.nazwa);
            this.typ=source.typ;

        }
        public Object get(int index){
            return dane.get(index);
        }

        void add(Object o){
            dane.add(o);
        }
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
    }

    protected Kolumna[] kolumny;
    protected int rowNumber;

    //todo: implement header true- false
    protected void readFile(String path, boolean header) throws IOException {
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
    public DataFrame(String path,String[] typykolumn,boolean header) throws IOException{
        kolumny=new Kolumna[typykolumn.length];
        for(int i=0;i<typykolumn.length;i++)
            kolumny[i]=new Kolumna("",DataType.getDataType(typykolumn[i]));
        readFile(path,header);
    }
    protected DataFrame(int count){
        kolumny=new Kolumna[count];
        rowNumber=0;
    }
    public DataFrame(String[] nazwyKolumn,DataType[] typyKolumn){
        construct(nazwyKolumn,typyKolumn);
    }
    public DataFrame(String[] nazwyKolumn,String[] typyKolumn){
        DataType[] types=new DataType[typyKolumn.length];
        for(int i =0; i<typyKolumn.length;i++)
            types[i]=DataType.getDataType(typyKolumn[i]);
        construct(nazwyKolumn,types);
    }
    protected void construct(String[] nazwyKolumn,DataType[] typyKolumn){
        kolumny=new Kolumna[typyKolumn.length];
        for(int i =0; i<typyKolumn.length;i++)
            kolumny[i]=new Kolumna(nazwyKolumn[i],typyKolumn[i]);
        rowNumber=0;
    }


    public DataFrame(Kolumna[] kolumny){
        this.kolumny=kolumny;
        rowNumber=kolumny[0].dane.size();
        for(Kolumna k:kolumny)
            if(k.dane.size()!=rowNumber)
                throw new RuntimeException("this shouldn't happen");
    }

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

    public Kolumna get(String colname){
        for (Kolumna k:kolumny)
            if(k.nazwa.equals(colname))
                return  k;

        throw new NoSuchElementException("No such column: "+colname);
    }
    //todo: get record
    //todo: override for Sparse
    public DataFrame get(String[] cols,boolean copy){
        Kolumna[] kolumny = new Kolumna[cols.length];

        for(int i=0;i<cols.length;i++)
            if(copy)
                kolumny[i]=new Kolumna(get(cols[i]));
            else
                kolumny[i]=get(cols[i]);

        return new DataFrame(kolumny);
    }

    public DataFrame iloc(int i){

        return iloc(i,i);
    }

    public void addRecord(Object... vals){
        if(vals.length!=kolumny.length)
            throw new RuntimeException("This shoudn't happen, but i can see why could");
        int i=0;
        rowNumber++;
        for(Kolumna k:kolumny)
            k.add(vals[i++]);

    }

    public DataFrame iloc(int from ,int to){
        if(from<0 || from>=rowNumber)
            throw new IndexOutOfBoundsException("No such index: "+from);

        if(to<0 || to>=rowNumber)
            throw new IndexOutOfBoundsException("No such index: "+to);

        if(to<from)
            throw new IndexOutOfBoundsException("unable to create range from "+from+" to "+to);

        String[] typy = new String[kolumny.length];
        String[] nazwy = new String[kolumny.length];
        for(int i=0;i<kolumny.length;i++) {
            nazwy[i] = new String(kolumny[i].nazwa);
            typy[i] = kolumny[i].typ.id;
        }

        DataFrame df=new DataFrame(nazwy,typy);
        Object[] temp=new Object[kolumny.length];

        for(int i=from;i<=to;i++){
            int j=0;
            for(Kolumna k:kolumny)
               temp[j++]=k.dane.get(i);

            df.addRecord(temp);
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
