package test;

import lab0.dataframe.DataFrame;
import lab0.dataframe.DataFrame.Grupator4000;
import lab0.dataframe.SparseDataFrame;
import lab0.dataframe.exceptions.DFApplyableException;
import lab0.dataframe.exceptions.DFColumnTypeException;
import lab0.dataframe.values.*;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class GroupbyTest {
    static DataFrame frameiddate;
    static DataFrame frameid;
    static DataFrame multidata;
    @BeforeAll
    static void setUp() {
        try {
            frameiddate = new SparseDataFrame("test/testData/multi/groupby.csv",new Value[]{new StringValue("a"),new DateTimeValue("1985-02-04"),new DoubleValue(0.3935550074650053),new DoubleValue(-979.0616718111498)});
            frameid = new SparseDataFrame("test/testData/single/groupby.csv",new Value[]{new StringValue("a"),new DateTimeValue("1985-02-04"),new DoubleValue(0.3935550074650053),new DoubleValue(-979.0616718111498)});
            System.out.println("multi:"+frameiddate.size());
            System.out.println("single:"+frameid.size());
            multidata = new DataFrame("test/testData/groupedData.csv",new Class[]{StringValue.class,StringValue.class,IntegerValue.class,DoubleValue.class, FloatValue.class,DateTimeValue.class,});
        } catch (IOException e) {
            e.printStackTrace();
        } catch (DFColumnTypeException e) {
            e.printStackTrace();
        }
    }

    Grupator4000 grupbyid;
    Grupator4000 grupbyiddate;
    Grupator4000 multidataid;
    Grupator4000 multidataidstring;
    Grupator4000 multidataidint;
    @BeforeEach
    void setGrupby() throws DFColumnTypeException, CloneNotSupportedException {
        grupbyid = frameid.groupBy("id");
        grupbyiddate = frameiddate.groupBy("id","date");
        multidataid = multidata.groupBy("id");
        multidataidstring = multidata.groupBy("string","id");
        multidataidint = multidata.groupBy("id","int");
    }

    @Test
    void sum() {
        try {
            DataFrame sum_iddate= new DataFrame("test/testData/multi/sum.csv",new Class[]{StringValue.class,DateTimeValue.class,DoubleValue.class,DoubleValue.class});
            DataFrame sum_id= new DataFrame("test/testData/single/sum.csv",new Class[]{StringValue.class,DoubleValue.class,DoubleValue.class});
            assertEquals(sum_iddate,grupbyiddate.sum());
            assertEquals(sum_id,grupbyid.sum());
            System.out.println(multidataid.sum());
            System.out.println(multidataidstring.sum());
            System.out.println(multidataidint.sum());

        } catch (IOException e) {
            e.printStackTrace();
        } catch (DFColumnTypeException e) {
            e.printStackTrace();
        } catch (DFApplyableException e) {
            e.printStackTrace();
        }
    }

    @Test
    void mean() {
        try {
            DataFrame sum_iddate= new DataFrame("test/testData/multi/mean.csv",new Class[]{StringValue.class,DateTimeValue.class,DoubleValue.class,DoubleValue.class});
            DataFrame sum_id= new DataFrame("test/testData/single/mean.csv",new Class[]{StringValue.class,DoubleValue.class,DoubleValue.class});

            assertEquals(sum_iddate,grupbyiddate.mean());
            assertEquals(sum_id,grupbyid.mean());
            System.out.println(multidataid.mean());
            System.out.println(multidataidstring.mean());
            System.out.println(multidataidint.mean());

        } catch (IOException e) {
            e.printStackTrace();
        } catch (DFColumnTypeException e) {
            e.printStackTrace();
        } catch (DFApplyableException e) {
            e.printStackTrace();
        }
    }

    @Test
    void var() {
        try {
            DataFrame sum_iddate= new DataFrame("test/testData/multi/var.csv",new Class[]{StringValue.class,DateTimeValue.class,DoubleValue.class,DoubleValue.class});
            DataFrame sum_id= new DataFrame("test/testData/single/var.csv",new Class[]{StringValue.class,DoubleValue.class,DoubleValue.class});

            assertEquals(sum_id,grupbyid.var());
            assertEquals(sum_iddate,grupbyiddate.var());
            System.out.println(multidataid.var());
            System.out.println(multidataidstring.var());
            System.out.println(multidataidint.var());

        } catch (IOException e) {
            e.printStackTrace();
        } catch (DFColumnTypeException e) {
            e.printStackTrace();
        } catch (DFApplyableException e) {
            e.printStackTrace();
        }
    }

    @Test
    void std() {
        try {
            DataFrame sum_iddate= new DataFrame("test/testData/multi/std.csv",new Class[]{StringValue.class,DateTimeValue.class,DoubleValue.class,DoubleValue.class});
            DataFrame sum_id= new DataFrame("test/testData/single/std.csv",new Class[]{StringValue.class,DoubleValue.class,DoubleValue.class});

            assertEquals(sum_iddate,grupbyiddate.std());
            assertEquals(sum_id,grupbyid.std());
            System.out.println(multidataid.std());
            System.out.println(multidataidstring.std());
            System.out.println(multidataidint.std());

        } catch (IOException e) {
            e.printStackTrace();
        } catch (DFColumnTypeException e) {
            e.printStackTrace();
        } catch (DFApplyableException e) {
            e.printStackTrace();
        }
    }

    @Test
    void min() {
        try {
            DataFrame sum_iddate= new DataFrame("test/testData/multi/min.csv",new Class[]{StringValue.class,DateTimeValue.class,DoubleValue.class,DoubleValue.class});
            DataFrame sum_id= new DataFrame("test/testData/single/min.csv",new Class[]{StringValue.class,DateTimeValue.class,DoubleValue.class,DoubleValue.class});

            assertEquals(sum_iddate,grupbyiddate.min());
            assertEquals(sum_id,grupbyid.min());
            System.out.println(multidataid.min());
            System.out.println(multidataidstring.min());
            System.out.println(multidataidint.min());

        } catch (IOException e) {
            e.printStackTrace();
        } catch (DFColumnTypeException e) {
            e.printStackTrace();
        } catch (DFApplyableException e) {
            e.printStackTrace();
        }
    }

    @Test
    void max() {
        try {
            DataFrame sum_iddate= new DataFrame("test/testData/multi/max.csv",new Class[]{StringValue.class,DateTimeValue.class,DoubleValue.class,DoubleValue.class});
            DataFrame sum_id= new DataFrame("test/testData/single/max.csv",new Class[]{StringValue.class,DateTimeValue.class,DoubleValue.class,DoubleValue.class});

            assertEquals(sum_iddate,grupbyiddate.max());
            assertEquals(sum_id,grupbyid.max());
            System.out.println(multidataid.max());
            System.out.println(multidataidstring.max());
            System.out.println(multidataidint.max());

        } catch (IOException e) {
            e.printStackTrace();
        } catch (DFColumnTypeException e) {
            e.printStackTrace();
        } catch (DFApplyableException e) {
            e.printStackTrace();
        }
    }


}
