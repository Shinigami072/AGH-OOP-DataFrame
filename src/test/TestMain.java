import lab0.dataframe.DataFrame;
import lab0.dataframe.DataFrameDB;
import lab0.dataframe.exceptions.DFApplyableException;
import lab0.dataframe.exceptions.DFColumnTypeException;
import lab0.dataframe.exceptions.DFValueBuildException;
import lab0.dataframe.groupby.GroupBy;
import lab0.dataframe.values.*;

import javax.sound.midi.Soundbank;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.sql.SQLException;
import java.util.Random;

class TestMain {

    public static void main(String[] argv) throws IOException, DFColumnTypeException, DFApplyableException, CloneNotSupportedException, SQLException, ClassNotFoundException, DFValueBuildException {
//        Class.forName("com.mysql.jdbc.Driver");
        DataFrameDB db = DataFrameDB.getBuilder()
                .setUrl("jdbc:mysql://mysql.agh.edu.pl/krzysst")
                .setLogin("krzysst", "2JuPF0y9TSCvuUsL")
                .setName("city").build();
//        DataFrameDB db =DataFrameDB.getBuilder()
//                .setUrl("jdbc:mysql://ensembldb.ensembl.org/rattus_norvegicus_core_70_5")
//                .setLogin("anonymous","")
//                .setName("marker_feature").build();

//        for(int i=0;i<db.size();i++){
//        System.out.println(Arrays.toString(db.getRecord(i)));
//        }
        final int N = 7;
        Random r = new Random();
        System.out.println("DB start");

        long mean1 = System.currentTimeMillis();
        for (int i = 0; i < N; i++) {
            GroupBy gb = db.groupBy(db.getNames()[r.nextInt(db.getColCount())]);
            gb.std();
        }
        mean1 = System.currentTimeMillis() - mean1;
        System.out.println("DB finished");

        DataFrame db_ = db.toDataFrame();
        long mean2 = System.currentTimeMillis();
        for (int i = 0; i < N; i++) {
            GroupBy gb = db_.groupBy(db_.getNames()[r.nextInt(db.getColCount())]);
            gb.std();
        }

        mean2 = System.currentTimeMillis() - mean2;
        System.out.println("DF finished");


        System.out.printf("%d : %d", mean1, mean2);

//        System.out.println(db.size());
//        System.out.println(db);
//        DataFrame.Column k = db.get(db.getNames()[0]);
//        System.out.println(k);
//        System.out.println(k.get(0));
//        System.out.println(db.iloc(1,4));
//        System.out.println(db.iloc(1));
//        try {
//            db.addRecord(new StringValue("1234567891251"), new StringValue("This Lock"), new StringValue("Anton Sokolov"), new IntegerValue(2018));
//        }catch (Exception ignore){}
//        System.out.println(db);
//
//        System.out.println(k.get(0));
//        GroupBy grp = db.groupBy("isbn");
//
//
//        System.out.println(grp.max());
//        System.out.println(grp.min());
//        System.out.println(grp.mean());
//        System.out.println(grp.sum());
//        System.out.println(grp.std());
//        System.out.println(grp.var());
//        System.out.println(grp.apply(new VarApplyable()));


//        DataFrame df = new DataFrame(new String[]{"A", "B", "C"}, new Class[]{StringValue.class, IntegerValue.class, FloatValue.class});
//        System.out.println(df);
//        df.addRecord(new StringValue("A"), new IntegerValue(15),new FloatValue( 17.0f));
//        df.addRecord(new StringValue("B"), new IntegerValue(5), new FloatValue(1.0f));
//        df.addRecord(new StringValue("C"), new IntegerValue(4), new FloatValue(7.0f));
//        df.addRecord(new StringValue("D"), new IntegerValue(5), new FloatValue(7.5f));
//        System.out.println(df);
////
//        System.out.println(df.get("A"));
//        System.out.println(df.get("B"));
//        System.out.println(df.get("C"));
////
////        System.out.println(df.iloc(1));
////
////        System.out.println(df.iloc(2, 3));
////
////        System.out.println(df.get(new String[]{"A", "B"}, false));
////        System.out.println(df.get(new String[]{"A", "B"}, true));
////        System.out.println(df.get(new String[]{"A", "B"}, true) != df.get(new String[]{"A", "B"}, false));
////        DataFrame df =new DataFrame("sparse.csv", new String[]{"float","float","float"});
////        System.out.println(df.iloc(100));
////
////        SparseDataFrame sf=new SparseDataFrame(df,new Object[]{0.0f,0.0f,0.0f});//new SparseDataFrame("sparse.csv", new String[]{"float","float","float"},new Object[]{0.0f,0.0f,0.0f});
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

  }

