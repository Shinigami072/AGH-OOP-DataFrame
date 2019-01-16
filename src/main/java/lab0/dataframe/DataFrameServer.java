package lab0.dataframe;

import lab0.dataframe.exceptions.DFApplyableException;
import lab0.dataframe.exceptions.DFColumnTypeException;
import lab0.dataframe.groupby.Applyable;
import lab0.dataframe.groupby.GroupBy;
import lab0.dataframe.server.protocol.ApplyOperation;
import lab0.dataframe.server.protocol.ClientProtocolParser;
import lab0.dataframe.server.protocol.ClientRequestType;
import lab0.dataframe.server.protocol.PortType;
import lab0.dataframe.values.Value;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class DataFrameServer extends DataFrameSparse implements AutoCloseable {

    private transient Socket comm;
    private transient ClientProtocolParser parser;

    public DataFrameServer(String[] nazwyKolumn, Value[] hidden, String host, int port) throws IOException {
        super(nazwyKolumn, hidden);
        comm = new Socket(host, port);
        ObjectInputStream in = new ObjectInputStream(comm.getInputStream());
        ObjectOutputStream out= new ObjectOutputStream(comm.getOutputStream());
        parser = new ClientProtocolParser(in,out);
    }
    public DataFrameServer(String[] nazwyKolumn, Value[] hidden, String host) throws IOException {
        this(nazwyKolumn, hidden, host, PortType.CLIENT.getPort());
    }

    public DataFrameServer(DataFrame df, String host) throws IOException {
        this(df, host, PortType.CLIENT.getPort());
    }

    public DataFrameServer(DataFrame df, String host, int port) throws IOException {
        this(df.getNames(), df.getRecord(0), host, port);
        for (int i = 0; i < df.size(); i++) {
            try {
                addRecord(df.getRecord(i));
            } catch (DFColumnTypeException e) {
                e.printStackTrace();
            }
        }
        optimizeStorage();
    }

    @Override
    public void close() throws IOException {
        parser.writeRequestType(ClientRequestType.DISCONNECT);

        comm.close();
    }

    @Override
    public GroupBy groupBy(String... colname) {
        Set<String> names = new HashSet<String>(Arrays.asList(getNames()));
        if(names.containsAll(Arrays.asList(colname)))
            return new GroupHolderServer(this,colname);
        else
            throw new IllegalArgumentException("does not contain column");
    }

    class GroupHolderServer implements GroupBy {
        private final DataFrame dataFrame;
        private final String[] colname;


        public GroupHolderServer(DataFrame dataFrame, String[] colname) {
            this.dataFrame = dataFrame;
            this.colname = colname;
        }


        @Override
        public DataFrame max() throws DFApplyableException {
            return serverSend(ApplyOperation.MAX);

        }

        @Override
        public DataFrame min() throws DFApplyableException {
            return serverSend(ApplyOperation.MIN);

        }

        @Override
        public DataFrame mean() throws DFApplyableException {
            return serverSend(ApplyOperation.MEAN);

        }

        @Override
        public DataFrame std() throws DFApplyableException {
            return serverSend(ApplyOperation.STD);

        }

        @Override
        public DataFrame sum() throws DFApplyableException {
            return serverSend(ApplyOperation.SUM);

        }

        @Override
        public DataFrame var() throws DFApplyableException {
            return serverSend(ApplyOperation.VAR);

        }

        public DataFrame serverSend(ApplyOperation type) throws DFApplyableException {
            try {
                System.out.println("sending Request");
                parser.writeRequestType(ClientRequestType.APPLY_GROUP);
                parser.writeApplyType(type);
                parser.writeColnames(colname);
                parser.writeInt(dataFrame.hashCode());
                parser.flush();
                System.out.println("sent Request");
                System.out.println("sending Dataframe");
                if (!parser.readBoolean()) {
                    parser.writeDataFrame(dataFrame);
                    parser.flush();
                }
                System.out.println("sent Dataframe");

                return parser.readDataFrame();
            } catch (IOException e) {
                e.printStackTrace();
                throw new DFApplyableException(e.getMessage());
            }
        }

        @Override
        public DataFrame apply(Applyable apply) throws DFApplyableException {
            throw new UnsupportedOperationException("unable to use server acceleration for generic applyable");
        }
    }
}
