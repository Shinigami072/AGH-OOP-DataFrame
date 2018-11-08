package test;

import lab0.dataframe.DataFrame;
import lab0.dataframe.values.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;
class TestDF {

    private DataFrame df;
    private String[] names;
    private Class<? extends Value>[] types;
    private StringValue[] str;
    private IntegerValue[] ints;
    private FloatValue[]  floats;

    @BeforeEach
    void setUp() {
        System.out.println("Test Setup");
        String[] names = {"A","B","C"};
        Class<? extends Value>[] types = new Class[]{StringValue.class, IntegerValue.class, FloatValue.class};

        StringValue[] str = {new StringValue("A"),new StringValue("B"),new StringValue("C"),new StringValue("D")};
        IntegerValue[] ints = {new IntegerValue(15),new IntegerValue(5),new IntegerValue(4),new IntegerValue(5)};
        FloatValue[] floats = {new FloatValue(17.0f),new FloatValue(1.0f),new FloatValue(7.0f),new FloatValue(7.5f)};
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
                "|A             :    StringValue|B             :   IntegerValue|C             :     FloatValue|\n" +
                        "|                             A|                            15|                          17.0|\n" +
                        "|                             B|                             5|                           1.0|\n" +
                        "|                             C|                             4|                           7.0|\n" +
                        "|                             D|                             5|                           7.5|\n",df.toString(),"ToString()");

        assertArrayEquals(df.getNames(),names);
        assertArrayEquals(df.getTypes(),types);
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
        assertEquals(b1,a1);
        assertEquals(a1,b1);
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
        assertEquals(b2,a2);
        assertEquals(a2,b2);
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
        Class<? extends Value>[] types = new Class[]{IntegerValue.class, DoubleValue.class, StringValue.class};
        IntegerValue[] colI = {new IntegerValue(0),new IntegerValue(1),new IntegerValue(2),new IntegerValue(3),new IntegerValue(4),new IntegerValue(5)};
        DoubleValue[] colII = {new DoubleValue(0.5),new DoubleValue(0.4),new DoubleValue(0.3),new DoubleValue(0.2),new DoubleValue(0.1),new DoubleValue(0.0)};
        StringValue[] colIII = {new StringValue("A"),new StringValue("B"),new StringValue("C"),new StringValue("D"),new StringValue("E"),new StringValue("F")};

        DataFrame dfF = new DataFrame("test/testData/test.csv",types);

        for(int i=0;i<cols.length;i++){
            Assertions.assertEquals(colI[i],dfF.get(cols[0]).get(i));
            Assertions.assertEquals(colII[i],dfF.get(cols[1]).get(i));
            Assertions.assertEquals(colIII[i],dfF.get(cols[2]).get(i));
        }


        DataFrame dfH = new DataFrame("test/testData/test-noH.csv",types,cols);

        for(int i=0;i<cols.length;i++){
            Assertions.assertEquals(colI[i],dfH.get(cols[0]).get(i));
            Assertions.assertEquals(colII[i],dfH.get(cols[1]).get(i));
            Assertions.assertEquals(colIII[i],dfH.get(cols[2]).get(i));
        }



    }


    private DataFrame create(String[] names,  Class<? extends Value>[] types, Value[]... arrays){

        DataFrame df = new DataFrame(names,types);
        Value[] v =new Value[types.length];
        for(int i=0;i<arrays[0].length;i++) {
            for (int j = 0; j < types.length; j++)
                v[j] = arrays[j][i];
            df.addRecord(v);
        }
        return df;
    }
}

