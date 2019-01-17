package lab0.dataframe.server;

import lab0.dataframe.DataFrame;
import lab0.dataframe.DataFrameServer;
import lab0.dataframe.DataFrameSparse;
import lab0.dataframe.exceptions.DFApplyableException;
import lab0.dataframe.exceptions.DFColumnTypeException;
import lab0.dataframe.exceptions.DFValueBuildException;
import lab0.dataframe.server.protocol.PortType;
import lab0.dataframe.values.*;

import java.io.IOException;

public class clientTaskTest {
    public static void main(String... args) throws IOException, DFColumnTypeException, CloneNotSupportedException, DFApplyableException, DFValueBuildException {

        String tablename = "large_groupby";
        String path = "test/testData/ultimate/" + tablename + ".csv";

        DataFrame df= new DataFrame(path, new Class[]{IntegerValue.class, DateTimeValue.class, StringValue.class, DoubleValue.class, FloatValue.class});
        DataFrameSparse sdf = new DataFrameSparse(df, df.getRecord(0));
        sdf.optimizeStorage();
        //OPtions setup
        if (args.length != 1)
            throw new IllegalArgumentException("requires server IP");
        System.out.println(args[0]);
        int port = args[0].indexOf(':');
        if (port > 0)

        {
            PortType.CLIENT.setPort(Integer.parseInt(args[0].substring(port)));
        }

        port = port < 0 ? args[0].length() : port;

        DataFrame testDF = new DataFrameServer(sdf,args[0].substring(0, port));
        System.out.println(testDF.groupBy("id"));
        System.out.println(testDF.groupBy("id").mean());
        System.out.println(testDF.groupBy("id").max());
        System.out.println(testDF.groupBy("id").min());
        System.out.println(testDF.groupBy("id").sum());
        System.out.println(testDF.groupBy("id").std());
        System.out.println(testDF.groupBy("id").var());
        ((DataFrameServer) testDF).close();
//        //Connect to server;
//        System.out.println("sock");
//        Socket sock = new Socket(args[0].substring(0, port), PortType.CLIENT.getPort());
//        ObjectOutputStream out = new ObjectOutputStream(sock.getOutputStream());
//        ObjectInputStream in = new ObjectInputStream(sock.getInputStream());
//        ClientProtocolParser parser = new ClientProtocolParser(in, out);
//
//        System.out.println("apply");
//        parser.writeRequestType(ClientRequestType.APPLY);
//        parser.writeApplyType(ApplyOperation.MAX);
//        parser.writeInt(sdf.hashCode());
//        parser.flush();
//        System.out.println("flush");
//
//        boolean b = parser.readBoolean();
//        System.out.println(b);
//        if (!b) {
//            parser.writeDataFrame(sdf);
//            parser.flush();
//        }
//        DataFrame df1 = parser.writeDataFrame();
//        System.out.println(" group");
//        parser.writeRequestType(ClientRequestType.GROUP);
//        parser.writeColnames(new String[]{"a"});
//        parser.writeInt(sdf.hashCode());
//        parser.flush();
//        if (!parser.readBoolean()) {
//            parser.writeDataFrame(sdf);
//            parser.flush();
//        }
//        System.out.println(parser.readMap());
//        System.out.println(" group");
//        parser.writeRequestType(ClientRequestType.GROUP);
//        parser.writeColnames(new String[]{"b"});
//        parser.writeInt(sdf.hashCode());
//        parser.flush();
//        if (!parser.readBoolean()) {
//            parser.writeDataFrame(sdf);
//            parser.flush();
//        }
//        System.out.println(parser.readMap());
//        System.out.println(" group");
//        parser.writeRequestType(ClientRequestType.GROUP);
//        parser.writeColnames(new String[]{"a"});
//        parser.writeInt(sdf.hashCode());
//        parser.flush();
//        if (!parser.readBoolean()) {
//            parser.writeDataFrame(sdf);
//            parser.flush();
//        }
//        System.out.println(parser.readMap());
//
//        System.out.println("apply group");
//        parser.writeRequestType(ClientRequestType.APPLY_GROUP);
//        parser.writeApplyType(ApplyOperation.STD);
//        parser.writeColnames(new String[]{"a"});
//        parser.writeInt(sdf.hashCode());
//        parser.flush();
//        if (!parser.readBoolean()) {
//            parser.writeDataFrame(sdf);
//            parser.flush();
//        }
//        System.out.println(parser.writeDataFrame());
//        parser.writeRequestType(ClientRequestType.DISCONNECT);
    }
}
