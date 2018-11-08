package test;

import lab0.dataframe.DataFrame;
import lab0.dataframe.DataFrame.Grupator4000;
import lab0.dataframe.SparseDataFrame;
import lab0.dataframe.values.DateTimeValue;
import lab0.dataframe.values.DoubleValue;
import lab0.dataframe.values.StringValue;
import lab0.dataframe.values.Value;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class GroupbyTest {
    static DataFrame frameiddate;
    static DataFrame frameid;
    @BeforeAll
    static void setUp() {
        try {
            frameiddate = new SparseDataFrame("test/testData/multi/groupby.csv",new Class[]{StringValue.class,DateTimeValue.class,DoubleValue.class,DoubleValue.class},new Value[]{new StringValue("a"),new DateTimeValue("1985-02-04"),new DoubleValue(0.3935550074650053),new DoubleValue(-979.0616718111498)});
            frameid = new SparseDataFrame("test/testData/single/groupby.csv",new Class[]{StringValue.class,DateTimeValue.class,DoubleValue.class,DoubleValue.class},new Value[]{new StringValue("a"),new DateTimeValue("1985-02-04"),new DoubleValue(0.3935550074650053),new DoubleValue(-979.0616718111498)});
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    Grupator4000 grupbyid;
    Grupator4000 grupbyiddate;
    @BeforeEach
    void setGrupby() {
        grupbyid = frameid.groupBy("id");
        grupbyiddate = frameiddate.groupBy("id","date");
    }

    @Test
    void sum() {
        try {
            DataFrame sum_iddate= new DataFrame("test/testData/multi/sum.csv",new Class[]{StringValue.class,DateTimeValue.class,DoubleValue.class,DoubleValue.class});
            DataFrame sum_id= new DataFrame("test/testData/single/sum.csv",new Class[]{StringValue.class,DoubleValue.class,DoubleValue.class});
            assertEquals(sum_iddate,grupbyiddate.sum());
            assertEquals(sum_id,grupbyid.sum());

        } catch (IOException e) {
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

        } catch (IOException e) {
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
        } catch (IOException e) {
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
        } catch (IOException e) {
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
        } catch (IOException e) {
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
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
