import lab0.dataframe.DataFrame;
import lab0.dataframe.DataFrameDB;
import lab0.dataframe.DataFrameThreaded;
import lab0.dataframe.exceptions.DFApplyableException;
import lab0.dataframe.exceptions.DFColumnTypeException;
import lab0.dataframe.exceptions.DFValueBuildException;
import lab0.dataframe.groupby.GroupBy;
import lab0.dataframe.values.*;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@SuppressWarnings("ALL")
class TestMain {


    /*todo:
    * DataFrameProxy
    * DataFrameNode
    * OPerations:
    * -groupby
    * -min/max/mean/...
    *
    * //storeInfo
    * //sendInfo
    * //serializarion
    * //tasksplitting
    * //NodeManagement
    *
    *
    *
    * C -> Proxy
    *
    * Proxy -> Node
    */
    public static void main(String[] argv) throws IOException, DFColumnTypeException, DFApplyableException, CloneNotSupportedException, SQLException, DFValueBuildException {
//        Class.forName("com.mysql.jdbc.Driver");
//        DataFrameDB dbA = DataFrameDB.getBuilder()
//                .setUrl("jdbc:mysql://mysql.agh.edu.pl/krzysst")
//                .setLogin("krzysst", "2JuPF0y9TSCvuUsL")
//                .setName("`TABLE 5`").build();
//        DataFrameDB db =DataFrameDB.getBuilder()
//                .setUrl("jdbc:mysql://ensembldb.ensembl.org/rattus_norvegicus_core_70_5")
//                .setLogin("anonymous","")
//                .setName("marker_feature").build();
//
////        for(int i=0;i<db.size();i++){
//        System.out.println(Arrays.toString(db.getRecord(i)));
//        }
//        DataFrame db = new DataFrame("city.csv",new Class[]{IntegerValue.class,StringValue.class,StringValue.class,StringValue.class,IntegerValue.class});
        DataFrame dbA = null;
        GroupBy gA;
        long A;
        int N = 10;
        String tablename = "large_groupby";
        String path = "test/testData/ultimate/" + tablename + ".csv";
        String name = null;
        String id = "A";
        boolean b = false;


        String[] names = new String[]{"method", "constructor", "groupby_id_date", "groupby_id", "max", "mean", "std"};
        Class<? extends Value>[] types = new Class[]{StringValue.class, IntegerValue.class, IntegerValue.class, IntegerValue.class, IntegerValue.class, IntegerValue.class, IntegerValue.class};
        DataFrame results = new DataFrame(names, types);
        Value[] resultsRow = new Value[names.length];
        do {
            switch (id) {
                case "A":
                    name = "DataFrame";
                    break;
                case "B":
                    name = "Parallel";
                    break;
                case "C":
                    name = "DataBase";
                    break;
            }

            ExecutorService threadPoolC = Executors.newWorkStealingPool(4);
            int col = 0;
            resultsRow[col] = Value.builder(types[col]).build(name);
            col++;
            A = System.currentTimeMillis();
            switch (id) {
                case "A":
                    dbA = new DataFrame(path, new Class[]{IntegerValue.class, DateTimeValue.class, StringValue.class, DoubleValue.class, FloatValue.class});
                    break;
                case "B":
                    dbA = new DataFrameThreaded(threadPoolC, path, new Class[]{IntegerValue.class, DateTimeValue.class, StringValue.class, DoubleValue.class, FloatValue.class});
                    break;
                case "C":
                    dbA = DataFrameDB.getBuilder()
                            .setUrl("jdbc:mysql://mysql.agh.edu.pl/krzysst")
                            .setLogin("krzysst", "2JuPF0y9TSCvuUsL")
                            .setName("ultimate5").build();//.build(new FileReader(path),null,new Class[]{StringValue.class, DateTimeValue.class, DoubleValue.class, FloatValue.class});
                    break;
            }
            A = System.currentTimeMillis() - A;
            resultsRow[col] = Value.builder(types[col]).build(String.valueOf(A));
            col++;

            A = System.currentTimeMillis();
            gA = dbA.groupBy("id", "date");
            A = System.currentTimeMillis() - A;

            resultsRow[col] = Value.builder(types[col]).build(String.valueOf(A));
            col++;

            A = System.currentTimeMillis();
            gA = dbA.groupBy("id");
            A = System.currentTimeMillis() - A;

            resultsRow[col] = Value.builder(types[col]).build(String.valueOf(A));
            col++;


            A = System.currentTimeMillis();
            gA.max();
            A = System.currentTimeMillis() - A;

            resultsRow[col] = Value.builder(types[col]).build(String.valueOf(A));
            col++;


            A = System.currentTimeMillis();
            gA.mean();
            A = System.currentTimeMillis() - A;

            resultsRow[col] = Value.builder(types[col]).build(String.valueOf(A));
            col++;


            A = System.currentTimeMillis();
            gA.std();
            A = System.currentTimeMillis() - A;


            resultsRow[col] = Value.builder(types[col]).build(String.valueOf(A));
            col++;

            threadPoolC.shutdown();
            results.addRecord(resultsRow);
            switch (id) {
                case "A":
                    id = "B";
                    break;
                case "B":
                    id = "C";
                    break;
                case "C":
                    id = "A";
                    N--;
                    b = N < 0;
                    break;
            }
            System.out.println(N + "left");
        } while (!b);
        results.toCSV(System.out);
        try (OutputStream o = new FileOutputStream("out.csv")) {
            results.toCSV(o);
        }
        System.out.println(results.groupBy("method").mean());

//        threadPoolA.shutdown();
        //Executors.newWorkStealingPool(10);
//        String name = null;
//        String tablename = "large_groupby";
//        String path = "test/testData/ultimate/"+tablename+".csv";
//        String id = argv[0];
//        do {
//            System.out.print("\nloading");
//            long A = System.currentTimeMillis();
//            switch (id) {
//                case "C":
//                    name = "dbase";
//                    System.out.print(" " + name + ".");
//                    System.out.print(".");
//                    db = DataFrameDB.getBuilder()
//                            .setUrl("jdbc:mysql://mysql.agh.edu.pl/krzysst")
//                            .setLogin("krzysst", "2JuPF0y9TSCvuUsL")
//                            .setName("ultimate5").build();//.build(new FileReader(path),null,new Class[]{StringValue.class, DateTimeValue.class, DoubleValue.class, FloatValue.class});
//                    System.out.print(".");
//                break;
//                case "B":
//                    name = "single";
//                    System.out.print(" " + name + ".");
//                    System.out.print(".");
//                    db = new DataFrame(path, new Class[]{StringValue.class, DateTimeValue.class, DoubleValue.class, FloatValue.class});
//                    System.out.print(".");
//                    break;
//                case "A":
//                    name = "multi";
//                    System.out.print(" " + name + ".");
//                    System.out.print(".");
//                    db = new DataFrameThreaded(threadPoolC, path, new Class[]{StringValue.class, DateTimeValue.class, DoubleValue.class, FloatValue.class});
//                    System.out.print(".");
//                break;
//
////            threadPool.shutdown();
////            long C = System.currentTimeMillis();
////            for (int i = 0; i < 1; i++)
//////                dbC = new DataFrameThreaded(threadPoolC, "city.csv", new Class[]{IntegerValue.class, StringValue.class, StringValue.class, StringValue.class, IntegerValue.class});
////                dbC = new DataFrameThreaded(threadPoolC, "test/testData/ultimate/ultimate.csv", new Class[]{StringValue.class, DateTimeValue.class, DoubleValue.class, FloatValue.class});
////            System.out.printf("C %3.2f\n", ((System.currentTimeMillis() - C) / (float) 2));
//            }
//            System.out.printf(name + " %3.2f  ", (float)(System.currentTimeMillis() - A));
//
//            System.out.printf(name + " %3.2f  ",  (float)(System.currentTimeMillis() - A));
//
//            final int N = 10;
//            Random r = new Random();
//            System.out.println("\nloaded");
//            for (int j = 0; j < 4; j++) {
//                int col = r.nextInt(db.getColCount() - 2) + 1;
//                int Cc = db.getColCount();
////            String[] s = new String[col];
////            for (int i = 0; i < col; i++) {
////                s[i]=dbA.getNames()[r.nextInt(Cc)];
////            }
//                String[] s = new String[1];
////            s[0]="CountryCode";
//                s[0] = "id";
//                System.out.print("\n" + Arrays.toString(s));
////[id]single 7590.80
////[id]single 7997.40
////[id]single 8030.70
////[id]single 8346.90
//                GroupBy gB = db.groupBy(s);
//
//                long B = System.currentTimeMillis();
//                for (int i = 0; i < N; i++) {
//                    gB.max();
////                    System.out.println(gB.max());
//                }
//                System.out.printf(name + " %3.2f  ", ((System.currentTimeMillis() - B) / (float) N));
//            }
//        } while (id.equals("B") && (id = "A").equals("A"));
//


//[id]single 7139.00
//[id]single 8971.00
//[id]single 9063.50
//[id]single 8906.50
//[id]single 8866.00
//[id]single 8912.50
        //4 fixed
//[id]multi 12609.00
//[id]multi 13969.50
//[id]multi 14076.50
//[id]multi 14076.00
//[id]multi 13798.00
//[id]multi 13899.50
        //64 fixed
//[id]multi 11757.50
//[id]multi 14158.00
//[id]multi 13451.50
//[id]multi 13271.50
//[id]multi 13137.00
//[id]multi 13451.00
        //1 fixed
//[id]multi 14558.50
//[id]multi 14973.50
//[id]multi 15043.00
//[id]multi 15570.50
//[id]multi 15159.50
//[id]multi 16705.50

//cached
//[id]multi 13729.50
//[id]multi 14619.00
//[id]multi 13373.50
//[id]multi 13548.50
//[id]multi 13236.50
//[id]multi 13299.00
//if DataFrmae
//[id]multi 5101.00
//[id]multi 7173.50
//[id]multi 7686.50
//[id]multi 7873.00
//[id]multi 7323.50
//[id]multi 7304.50
    }
//        DataFrame gA = dbA.groupBy("CountryCode").max();
//        DataFrame gB = dbB.groupBy("CountryCode").max();
//        DataFrame gC = dbC.groupBy("CountryCode").max();
//
//        System.out.println(gA.equals(gB));
//        System.out.println(gA.equals(gC));
//        System.out.println(gB.equals(gC));

//        threadPoolA.shutdown();
//        System.out.println("GroupBy");
//        for (int j = 0; j < 4; j++) {
//            long B = System.currentTimeMillis();
//            for (int i = 0; i < 2; i++)
//                dbB = new DataFrame("test/testData/multi/groupby.csv", new Class[]{StringValue.class, DateTimeValue.class, DoubleValue.class, FloatValue.class});
//            System.out.printf("B %3.2f %d ", ((System.currentTimeMillis() - B) / (float) N),dbB.size());
//
//            ExecutorService threadPool = Executors.newFixedThreadPool(10);//Executors.newWorkStealingPool(10);
//            long A = System.currentTimeMillis();
//            for (int i = 0; i < 2; i++)
//                dbA = new DataFrameThreaded(threadPool, "test/testData/multi/groupby.csv", new Class[]{StringValue.class, DateTimeValue.class, DoubleValue.class, FloatValue.class});
//            System.out.printf("A %3.2f %d", ((System.currentTimeMillis() - A) / (float) N), dbA.size());
//            threadPool.shutdown();
//
//            threadPool = Executors.newWorkStealingPool(10);
//            long C = System.currentTimeMillis();
//            for (int i = 0; i < 2; i++)
//                dbA = new DataFrameThreaded(threadPool, "test/testData/multi/groupby.csv", new Class[]{StringValue.class, DateTimeValue.class, DoubleValue.class, FloatValue.class});
//            System.out.printf("C %3.2f %d\n", ((System.currentTimeMillis() - C) / (float) N), dbA.size());
//            threadPool.shutdown();
//        }
    //        DataFrameThreaded db1 = new DataFrameThreaded(threadPool,db);
//        System.out.println(db);
//        long cur = System.currentTimeMillis();
//        for (int i = 0; i < N; i++) {
//            int a = 2950;//r.nextInt(db.size());
//            int b = 2971;//r.nextInt(db.size());
////            System.out.println(db1.iloc(Math.min(a, b), Math.max(a, b)));
//
//        }
//        System.out.println(Arrays.toString(db.getRecord(20)));
//        A = System.currentTimeMillis();
//        dbA.groupBy("CountryCode").max();
//        System.out.println(System.currentTimeMillis() - A);
//        B = System.currentTimeMillis();
//        dbB.groupBy("CountryCode").max();
//        System.out.println(System.currentTimeMillis() - B);


//        String[] cols = db.getNames();
//        for (int i = 0; i < N; i++) {
//            db.get(new String[]{cols[r.nextInt(cols.length)], cols[r.nextInt(cols.length)]}, false);
//        }
//        System.out.println(System.currentTimeMillis() - cur);
//        System.out.println("DB start");
//
//        long mean1 = System.currentTimeMillis();
//        for (int i = 0; i < N; i++) {
//            GroupBy gb = db.groupBy(db.getNames()[r.nextInt(db.getColCount())]);
//            gb.std();
//        }
//        mean1 = System.currentTimeMillis() - mean1;
//        System.out.println("DB finished");
//        db.setAutoCommit(true);
//
//        DataFrame db_ = db.toDataFrame();
//        long mean2 = System.currentTimeMillis();
//        for (int i = 0; i < N; i++) {
//            GroupBy gb = db_.groupBy(db_.getNames()[r.nextInt(db.getColCount())]);
//            gb.std();
//        }
//
//        mean2 = System.currentTimeMillis() - mean2;
//        System.out.println("DF finished");
//
//
//        System.out.printf("%d : %d", mean1, mean2);
//
////        System.out.println(db.size());
////        System.out.println(db);
////        DataFrame.Column k = db.get(db.getNames()[0]);
////        System.out.println(k);
////        System.out.println(k.get(0));
////        System.out.println(db.iloc(1,4));
////        System.out.println(db.iloc(1));
////        try {
////            db.addAllRecords(new StringValue("1234567891251"), new StringValue("This Lock"), new StringValue("Anton Sokolov"), new IntegerValue(2018));
////        }catch (Exception ignore){}
////        System.out.println(db);
////
////        System.out.println(k.get(0));
////        GroupBy grp = db.groupBy("isbn");
////
////
////        System.out.println(grp.max());
////        System.out.println(grp.min());
////        System.out.println(grp.mean());
////        System.out.println(grp.sum());
////        System.out.println(grp.std());
////        System.out.println(grp.var());
////        System.out.println(grp.apply(new VarApplyable()));
//
//
////        DataFrame df = new DataFrame(new String[]{"A", "B", "C"}, new Class[]{StringValue.class, IntegerValue.class, FloatValue.class});
////        System.out.println(df);
////        df.addAllRecords(new StringValue("A"), new IntegerValue(15),new FloatValue( 17.0f));
////        df.addAllRecords(new StringValue("B"), new IntegerValue(5), new FloatValue(1.0f));
////        df.addAllRecords(new StringValue("C"), new IntegerValue(4), new FloatValue(7.0f));
////        df.addAllRecords(new StringValue("D"), new IntegerValue(5), new FloatValue(7.5f));
////        System.out.println(df);
//////
////        System.out.println(df.get("A"));
////        System.out.println(df.get("B"));
////        System.out.println(df.get("C"));
//////
//////        System.out.println(df.iloc(1));
//////
//////        System.out.println(df.iloc(2, 3));
//////
//////        System.out.println(df.get(new String[]{"A", "B"}, false));
//////        System.out.println(df.get(new String[]{"A", "B"}, true));
////        System.out.println(df.get(new String[]{"A", "B"}, true) != df.get(new String[]{"A", "B"}, false));
////        DataFrame df =new DataFrame("sparse.csv", new String[]{"float","float","float"});
////        System.out.println(df.iloc(100));
////
////        DataFrameSparse sf=new DataFrameSparse(df,new Object[]{0.0f,0.0f,0.0f});//new DataFrameSparse("sparse.csv", new String[]{"float","float","float"},new Object[]{0.0f,0.0f,0.0f});
////        System.out.println(sf.iloc(100));
//        Value.ValueBuilder b1 = Value.builder(StringValue.class);
//        Value t = b1.build("TEST");
//        System.out.println((t));
//        //assertEquals(df.get("a").get(0),0.0f);
////        System.out.println(sf);
//        for (int i = 0; i < 3; i++) {
//
//
//            DataFrame multi = new DataFrame("test/testData/multi/groupby.csv", new Class[]{StringValue.class, DateTimeValue.class, DoubleValue.class, DoubleValue.class});
////        System.out.println(multi);
//            DataFrame.Grupator4000 group = multi.groupBy("id", "date");
////        System.out.println(group.min());
//            System.out.println(group.std());
//        }
////        System.out.println(group.var());
////        System.out.println(group.std());


}



