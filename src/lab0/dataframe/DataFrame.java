package lab0.dataframe;

import java.util.ArrayList;
import java.util.NoSuchElementException;

public class DataFrame {
    class Kolumna{
        ArrayList dane;
        String nazwa;
        String typ;
        Kolumna(String nazwa,String typ){
            dane = new ArrayList();
            this.nazwa=nazwa;
            this.typ=typ;
        }
        Kolumna(Kolumna source){
            dane = new ArrayList(source.dane);
            this.nazwa=new String(source.nazwa);
            this.typ=new String(source.typ);
        }
    }

    private Kolumna[] kolumny;
    private int rowNumber;

    public DataFrame(String[] nazwyKolumn,String[] typyKolumn){
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

    public Kolumna get(String colname){
        for (Kolumna k:kolumny)
            if(k.nazwa.equals(colname))
                return  k;

        throw new NoSuchElementException("No such column: "+colname);
    }

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

    public void addRecord(Object[] vals){
        if(vals.length!=kolumny.length)
            throw new RuntimeException("This shoudn't happen, but i can see why could");
        int i=0;
        for(Kolumna k:kolumny)
            k.dane.add(vals[i++]);
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
            typy[i] = new String(kolumny[i].typ);
        }

        DataFrame df=new DataFrame(typy,nazwy);
        Object[] temp=new Object[kolumny.length];

        for(int i=from;i<to;i++){
            for(Kolumna k:kolumny)
               temp[i]=k.dane.get(i);

            df.addRecord(temp);
        }

        return df;
    }
}
