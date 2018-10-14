package test;

import lab0.dataframe.DataFrame;
import lab0.dataframe.DataType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.xml.crypto.Data;

import static org.junit.jupiter.api.Assertions.*;
class TestDF {

    @Test
    void testDF(){
        DataFrame df = new DataFrame(new String[]{"A","B","C"},new String[]{"string","int","float"});
        df.addRecord("A",15,17.0f);
        df.addRecord("B",5,1.0f);
        df.addRecord("C",4,7.0f);
        df.addRecord("D",5,7.5f);
        Assertions.assertEquals(
                "|A:string|B:int|C:float|\n" +
                        "|A|15|17.0|\n" +
                        "|B|5|1.0|\n" +
                        "|C|4|7.0|\n" +
                        "|D|5|7.5|\n",df.toString(),"ToString()");
        DataFrame.Kolumna kol = df.get("A");
        String[] str = {"A","B","C","D"};
        for(int i =0;i<kol.size();i++){
            Assertions.assertEquals(str[i],kol.get(i));
        }
        kol = df.get("B");
        int[] ints = {15,5,4,5};
        for(int i =0;i<kol.size();i++){
            Assertions.assertEquals(ints[i],kol.get(i));
        }
        kol = df.get("C");
        float[] floats = {17.0f,1.0f,7.0f,7.5f};
        for(int i =0;i<kol.size();i++){
            Assertions.assertEquals(floats[i],kol.get(i));
        }
        System.out.println(df.iloc(1));
        System.out.println(df.iloc(2,3));

        System.out.println(df.get(new String[]{"A","B"},false));
        System.out.println(df.get(new String[]{"A","B"},true));
        System.out.println(df.get(new String[]{"A","B"},true)!=df.get(new String[]{"A","B"},false));
        assertNotEquals(df.get(new String[]{"A","B"},true),(df.get(new String[]{"A","B"},true)));

    }
}

