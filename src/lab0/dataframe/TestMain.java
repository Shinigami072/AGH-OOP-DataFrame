package lab0.dataframe;


import lab0.dataframe.values.*;

import java.io.IOException;

class TestMain {

    public static void main(String[] argv) throws IOException {
        DataFrame df = new DataFrame(new String[]{"A", "B", "C"}, new Class[]{StringValue.class, IntegerValue.class, FloatValue.class});
        System.out.println(df);
        df.addRecord(new StringValue("A"), new IntegerValue(15),new FloatValue( 17.0f));
        df.addRecord(new StringValue("B"), new IntegerValue(5), new FloatValue(1.0f));
        df.addRecord(new StringValue("C"), new IntegerValue(4), new FloatValue(7.0f));
        df.addRecord(new StringValue("D"), new IntegerValue(5), new FloatValue(7.5f));
        System.out.println(df);
//
        System.out.println(df.get("A"));
        System.out.println(df.get("B"));
        System.out.println(df.get("C"));
//
//        System.out.println(df.iloc(1));
//
//        System.out.println(df.iloc(2, 3));
//
//        System.out.println(df.get(new String[]{"A", "B"}, false));
//        System.out.println(df.get(new String[]{"A", "B"}, true));
//        System.out.println(df.get(new String[]{"A", "B"}, true) != df.get(new String[]{"A", "B"}, false));
//        DataFrame df =new DataFrame("sparse.csv", new String[]{"float","float","float"});
//        System.out.println(df.iloc(100));
//
//        SparseDataFrame sf=new SparseDataFrame(df,new Object[]{0.0f,0.0f,0.0f});//new SparseDataFrame("sparse.csv", new String[]{"float","float","float"},new Object[]{0.0f,0.0f,0.0f});
//        System.out.println(sf.iloc(100));
        Value.ValueBuilder b1 = Value.builder(StringValue.class);
        Value t = b1.build("TEST");
        System.out.println((t));
        //assertEquals(df.get("a").get(0),0.0f);
//        System.out.println(sf);
        DataFrame multi = new DataFrame("groubymulti.csv",new Class[]{StringValue.class, DateTimeValue.class,DoubleValue.class,DoubleValue.class});
//        System.out.println(multi);
        DataFrame.Grupator4000 group = multi.groupBy("id","date");
//        System.out.println(group.min());
        System.out.println(group.std());

//        System.out.println(group.var());
//        System.out.println(group.std());

    }

  }

