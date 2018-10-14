package lab0.dataframe;

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

        Object hidden;
        int size;
        SparseKolumna(String nazwa, DataType typ,Object hidden) {
            super(nazwa, typ);
            this.hidden=hidden;
            size=0;
        }

        public SparseKolumna(SparseKolumna source) {
            super(source);
            this.hidden=source.hidden;
            size=source.size;
        }

        @Override
        void add(Object o) {
            if(!hidden.equals(o))
                super.add(new COOValue(size,o));
            size++;
        }

        @Override
        public Object get(int index) {

           //binary search
            int iR=dane.size()-1;
            int iL=0;
            int t,i;
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

    //todo: impement concrete versions
    //todo: toDense()
    public String toStringActual() {
        StringBuilder s=new StringBuilder();
        for(Kolumna k:kolumny)
            s.append("|"+k.nazwa+":"+k.typ.id);
        s.append("|\n");
        for(Kolumna k:kolumny){
                s.append("|"+k.dane.toString());
            s.append("|\n");
        }
        return s.toString();
    }
}

