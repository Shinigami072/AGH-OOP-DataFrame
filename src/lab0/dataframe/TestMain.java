package lab0.dataframe;

public class TestMain {

    public static void main(String[] argv){

        DataFrame df = new DataFrame(new String[]{"A","B","C"},new String[]{"A","B","C"});
        System.out.println(df);
        df.addRecord(new Object[]{"A",15,17.0f});
        df.addRecord(new Object[]{"B",5,1.0f});
        df.addRecord(new Object[]{"C",4,7.0f});
        df.addRecord(new Object[]{"D",5,7.5f});
        System.out.println(df);

        System.out.println(df.get("A"));
        System.out.println(df.get("B"));
        System.out.println(df.get("C"));

        System.out.println(df.iloc(1));

        System.out.println(df.iloc(2,3));

        System.out.println(df.get(new String[]{"A","B"},false));
        System.out.println(df.get(new String[]{"A","B"},true));

    }
}
