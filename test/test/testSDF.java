//package test;
//
//import lab0.dataframe.DataType;
//import lab0.dataframe.SparseDataFrame;
//import org.junit.jupiter.api.Assertions;
//import org.junit.jupiter.api.BeforeAll;
//import org.junit.jupiter.api.Test;
//
//import java.io.IOException;
//
//import static org.junit.jupiter.api.Assertions.*;
//class TestSDF {
//
//    private static SparseDataFrame df;
//    private static String[] names;
//    private static String[] types;
//    private static String[] str;
//    private static Integer[] ints;
//    private static Float[] floats;
//    private static Object[] hidden;
//
//    @BeforeAll
//    static void setUp() {
//        System.out.println("Test Setup");
//        String[] names1 = {"A","B","C"};
//        String[] types1= {"string","int","float"};
//        Object[] hidden1= {"A",0,1.0f};
//
//        String[] str1 = {"A","B","C","D","A","A","A","A","A","A","A","A","A","A","D"};
//        Integer[] ints1 = {15,5,4,5,0,0,0,0,0,0,0,0,0,0,0};
//        Float[] floats1 = {17.0f,1.0f,7.0f,7.5f,1.0f,17.0f,1.0f,7.0f,7.5f,1.0f,17.0f,1.0f,7.0f,7.5f,1.0f};
//        names=names1;
//        types=types1;
//
//        str=str1;
//        ints=ints1;
//        floats=floats1;
//        hidden=hidden1;
//        df = create(names,types,hidden,str,ints,floats);
//    }
//
//    @Test
//    void testToString() {
//        System.out.println("To String");
//        Assertions.assertEquals(
//                "|A:string|B:int|C:float|\n" +
//                        "|A|15|17.0|\n" +
//                        "|B|5|1.0|\n" +
//                        "|C|4|7.0|\n" +
//                        "|D|5|7.5|\n" +
//                        "|A|0|1.0|\n" +
//                        "|A|0|17.0|\n" +
//                        "|A|0|1.0|\n" +
//                        "|A|0|7.0|\n" +
//                        "|A|0|7.5|\n" +
//                        "|A|0|1.0|\n" +
//                        "|A|0|17.0|\n" +
//                        "|A|0|1.0|\n" +
//                        "|A|0|7.0|\n" +
//                        "|A|0|7.5|\n" +
//                        "|D|0|1.0|\n",df.toString(),"ToString()");
//
//        assertArrayEquals(df.getNames(),names);
//        DataType[] dataTypes = new DataType[types.length];
//        for(int i=0;i<types.length;i++)
//            dataTypes[i]=DataType.getDataType(types[i]);
//        assertArrayEquals(df.getTypes(),dataTypes);
//    }
//
//    @Test
//    void testGetKolumn() {
//        SparseDataFrame.SparseKolumna kol = df.get(names[0]);
//        for(int i =0;i<kol.size();i++){
//            Assertions.assertEquals(str[i],kol.get(i));
//        }
//        kol = df.get(names[1]);
//        for(int i =0;i<kol.size();i++){
//            Assertions.assertEquals(ints[i],kol.get(i));
//        }
//        kol =df.get(names[2]);
//        for(int i =0;i<kol.size();i++){
//            Assertions.assertEquals(floats[i],kol.get(i));
//        }
//    }
//
//    @SuppressWarnings("SuspiciousNameCombination")
//    @Test
//    void testIloc() {
//        for(int i=0;i<str.length;i++) {
//            SparseDataFrame row1 = df.iloc(i);
//            Assertions.assertEquals(str[i],row1.get(names[0]).get(0));
//            Assertions.assertEquals(ints[i],row1.get(names[1]).get(0));
//            Assertions.assertEquals(floats[i],row1.get(names[2]).get(0));
//        }
//
//        for(int x=0;x<str.length;x++)
//            for(//noinspection SuspiciousNameCombination
//                    int y=x;y<str.length;y++){
//                SparseDataFrame rows23 = df.iloc(x,y);
//                for(int i=x, j=0;i<y;i++,j++){
//                    Assertions.assertEquals(str[i],rows23.get(names[0]).get(j));
//                    Assertions.assertEquals(ints[i],rows23.get(names[1]).get(j));
//                    Assertions.assertEquals(floats[i],rows23.get(names[2]).get(j));
//                }
//            }
//    }
//
//    @Test
//    void testShallowCopy() {
//        SparseDataFrame a1 = df.get(new String[]{names[0],names[1]},false);
//        SparseDataFrame b1 = df.get(new String[]{names[0],names[1]},false);
//        for(int i=0;i<str.length;i++)
//        {
//            assertSame(a1.get(names[0]).get(i),b1.get(names[0]).get(i));
//            assertSame(a1.get(names[1]).get(i),b1.get(names[1]).get(i));
//        }
//    }
//
//    @Test
//    void testDeepCopy(){
//        SparseDataFrame a2 = df.get(new String[]{names[0],names[1]},true);
//        SparseDataFrame b2 = df.get(new String[]{names[0],names[1]},true);
//        assertNotSame(a2,b2);
//        for(int i=0;i<str.length;i++)
//        {
//            System.out.println(names[0]+i);
//            System.out.println(names[1]+i);
//            if(!a2.get(names[0]).get(i).equals(hidden[0]))
//                assertNotSame(a2.get(names[0]).get(i),b2.get(names[0]).get(i),names[0]+i );
//            if(!a2.get(names[1]).get(i).equals(hidden[1]))
//                assertNotSame(a2.get(names[1]).get(i),b2.get(names[1]).get(i),names[1]+i );
//        }
//
//    }
//
//    @Test
//    void testLoad() throws IOException{
//        String[] cols={"id","do","str"};
//        String[] typ={"int","double","string"};
//        Object[] hid={0,0.5,"A"};
//        int[] colI = {0,1,2,3,4,5};
//        double[] colII = {0.5,0.4,0.3,0.2,0.1,0.0};
//        String[] colIII = {"A","B","C","D","E","F"};
//
//        SparseDataFrame dfF = new SparseDataFrame("test.csv",typ,hid);
//
//        for(int i=0;i<cols.length;i++){
//            Assertions.assertEquals(colI[i],dfF.get(cols[0]).get(i));
//            Assertions.assertEquals(colII[i],dfF.get(cols[1]).get(i));
//            Assertions.assertEquals(colIII[i],dfF.get(cols[2]).get(i));
//        }
//
//
//        SparseDataFrame dfH = new SparseDataFrame("test-noH.csv",typ,hid,cols);
//
//        for(int i=0;i<cols.length;i++){
//            Assertions.assertEquals(colI[i],dfH.get(cols[0]).get(i));
//            Assertions.assertEquals(colII[i],dfH.get(cols[1]).get(i));
//            Assertions.assertEquals(colIII[i],dfH.get(cols[2]).get(i));
//        }
//
//
//
//    }
//
//
//    private static SparseDataFrame create(String[] names, String[] types, Object[] hidden, Object[]... arrays){
//
//        SparseDataFrame df = new SparseDataFrame(names,types,hidden);
//        Object[] v =new Object[types.length];
//        for(int i=0;i<arrays[0].length;i++) {
//            for (int j = 0; j < types.length; j++)
//                v[j] = arrays[j][i];
//            df.addRecord(v);
//        }
//        return df;
//    }
//}
//
