package lab0.dataframe;


public class TestMain {

    public static void main(String[] argv) throws Exception {
//        DataFrame df = new DataFrame(new String[]{"A", "B", "C"}, new String[]{"string", "int", "float"});
//        System.out.println(df);
//        df.addRecord("A", 15, 17.0f);
//        df.addRecord("B", 5, 1.0f);
//        df.addRecord("C", 4, 7.0f);
//        df.addRecord("D", 5, 7.5f);
//        System.out.println(df);
//
//        System.out.println(df.get("A"));
//        System.out.println(df.get("B"));
//        System.out.println(df.get("C"));
//
//        System.out.println(df.iloc(1));
//
//        System.out.println(df.iloc(2, 3));
//
//        System.out.println(df.get(new String[]{"A", "B"}, false));
//        System.out.println(df.get(new String[]{"A", "B"}, true));
//        System.out.println(df.get(new String[]{"A", "B"}, true) != df.get(new String[]{"A", "B"}, false));
        DataFrame df =new DataFrame("sparse.csv", new String[]{"float","float","float"});
        System.out.println(df.iloc(100));

        SparseDataFrame sf=new SparseDataFrame(df,new Object[]{0.0f,0.0f,0.0f});//new SparseDataFrame("sparse.csv", new String[]{"float","float","float"},new Object[]{0.0f,0.0f,0.0f});
        System.out.println(sf.iloc(100));

        //assertEquals(df.get("a").get(0),0.0f);
//        System.out.println(sf);

    }

  }

