package test;

import lab0.dataframe.DataFrame;
import lab0.dataframe.DataType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;
class TestDF {

    private DataFrame df;
    private String[] names;
    private String[] types;
    private String[] str;
    private Integer[] ints;
    private Float[] floats;

    @BeforeEach
    void setUp() {
        System.out.println("Test Setup");
        String[] names = {"A","B","C"};
        String[] types = {"string","int","float"};

        String[] str = {"A","B","C","D"};
        Integer[] ints = {15,5,4,5};
        Float[] floats = {17.0f,1.0f,7.0f,7.5f};
        this.names=names;
        this.types=types;

        this.str=str;
        this.ints=ints;
        this.floats=floats;

         df = create(names,types,str,ints,floats);
    }

    @Test
    void testToString() {
        System.out.println("To String");
        Assertions.assertEquals(
                "|A:string|B:int|C:float|\n" +
                        "|A|15|17.0|\n" +
                        "|B|5|1.0|\n" +
                        "|C|4|7.0|\n" +
                        "|D|5|7.5|\n",df.toString(),"ToString()");

        assertArrayEquals(df.getNames(),names);
        DataType[] dataTypes = new DataType[types.length];
        for(int i=0;i<types.length;i++)
            dataTypes[i]=DataType.getDataType(types[i]);
        assertArrayEquals(df.getTypes(),dataTypes);
    }

    @Test
    void testGetKolumn() {
        DataFrame.Kolumna kol = df.get(names[0]);
        for(int i =0;i<kol.size();i++){
            Assertions.assertEquals(str[i],kol.get(i));
        }
        kol = df.get(names[1]);
        for(int i =0;i<kol.size();i++){
            Assertions.assertEquals(ints[i],kol.get(i));
        }
        kol =df.get(names[2]);
        for(int i =0;i<kol.size();i++){
            Assertions.assertEquals(floats[i],kol.get(i));
        }
    }

    @Test
    void testIloc() {
        for(int i=0;i<str.length;i++) {
            DataFrame row1 = df.iloc(i);
            Assertions.assertEquals(str[i],row1.get(names[0]).get(0));
            Assertions.assertEquals(ints[i],row1.get(names[1]).get(0));
            Assertions.assertEquals(floats[i],row1.get(names[2]).get(0));
        }

        for(int x=0;x<str.length;x++)
            for(//noinspection SuspiciousNameCombination
                    int y=x;y<str.length;y++){
                DataFrame rows23 = df.iloc(x,y);
                for(int i=x, j=0;i<y;i++,j++){
                    Assertions.assertEquals(str[i],rows23.get(names[0]).get(j));
                    Assertions.assertEquals(ints[i],rows23.get(names[1]).get(j));
                    Assertions.assertEquals(floats[i],rows23.get(names[2]).get(j));
                }
            }
    }

    @Test
    void testShallowCopy() {
        DataFrame a1 = df.get(new String[]{names[0],names[1]},false);
        DataFrame b1 = df.get(new String[]{names[0],names[1]},false);
        for(int i=0;i<str.length;i++)
        {
            assertSame(a1.get(names[0]).get(i),b1.get(names[0]).get(i));
            assertSame(a1.get(names[1]).get(i),b1.get(names[1]).get(i));
        }
    }

    @Test
    void testDeepCopy(){
        DataFrame a2 = df.get(new String[]{names[0],names[1]},true);
        DataFrame b2 = df.get(new String[]{names[0],names[1]},true);
        assertNotSame(a2,b2);
        for(int i=0;i<str.length;i++)
        {
            System.out.println(names[0]+i);
            System.out.println(names[1]+i);
            assertNotSame(a2.get(names[0]).get(i),b2.get(names[0]).get(i),names[0]+i );
            assertNotSame(a2.get(names[1]).get(i),b2.get(names[1]).get(i),names[1]+i );
        }

    }

    @Test
    void testLoad() throws IOException{
        String[] cols={"id","do","str"};
        String[] typ={"int","double","string"};
        int[] colI = {0,1,2,3,4,5};
        double[] colII = {0.5,0.4,0.3,0.2,0.1,0.0};
        String[] colIII = {"A","B","C","D","E","F"};

        DataFrame dfF = new DataFrame("test.csv",typ);

        for(int i=0;i<cols.length;i++){
            Assertions.assertEquals(colI[i],dfF.get(cols[0]).get(i));
            Assertions.assertEquals(colII[i],dfF.get(cols[1]).get(i));
            Assertions.assertEquals(colIII[i],dfF.get(cols[2]).get(i));
        }


        DataFrame dfH = new DataFrame("test-noH.csv",typ,cols);

        for(int i=0;i<cols.length;i++){
            Assertions.assertEquals(colI[i],dfF.get(cols[0]).get(i));
            Assertions.assertEquals(colII[i],dfF.get(cols[1]).get(i));
            Assertions.assertEquals(colIII[i],dfF.get(cols[2]).get(i));
        }



    }


    private DataFrame create(String[] names, String[] types, Object[]... arrays){

        DataFrame df = new DataFrame(names,types);
        Object[] v =new Object[types.length];
        for(int i=0;i<arrays[0].length;i++) {
            for (int j = 0; j < types.length; j++)
                v[j] = arrays[j][i];
            df.addRecord(v);
        }
        return df;
    }
}

