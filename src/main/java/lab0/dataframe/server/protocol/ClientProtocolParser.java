package lab0.dataframe.server.protocol;

import lab0.dataframe.DataFrame;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Map;

public class ClientProtocolParser {
    private final ObjectInputStream in;
    private final ObjectOutputStream out;

    public ClientProtocolParser(ObjectInputStream in, ObjectOutputStream out) {
        this.in = in;
        this.out = out;
    }


    public ClientRequestType readRequestType() throws IOException {
        try {
            return (ClientRequestType) in.readObject();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            throw new IllegalStateException("no RequestType");
        }
    }
    public void writeRequestType(ClientRequestType type) throws IOException {
        out.writeObject(type);
    }

    public ApplyOperation readApplyType() throws IOException {
        try {
            return (ApplyOperation) in.readObject();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            throw new IllegalStateException("no ApplyOperation");
        }
    }

    public void writeApplyType(ApplyOperation type) throws IOException {
        out.writeObject(type);
    }

    public int readInt() throws IOException {
        return in.readInt();
    }

    public boolean readBoolean() throws IOException {
        return in.readBoolean();
    }

    public void writeBoolean(boolean b) throws IOException {
        out.writeBoolean(b);
    }

    public DataFrame readDataFrame() throws IOException {
        try {
            return (DataFrame) in.readObject();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            throw new IllegalStateException("no DF recieved");
        }
    }

    public void writeDataFrame(DataFrame df) throws IOException {
        out.writeObject(df);
    }

    public void flush() throws IOException {
        out.flush();
    }

    public void writeInt(int i) throws IOException {
        out.writeInt(i);
    }

    public String[] readColnames() throws IOException {
        try {
            return ((String[])in.readObject());
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            throw new IllegalStateException("no colnames recieved");
        }
    }
    public void writeColnames(String[] colnames) throws IOException {
        out.writeObject(colnames);
    }

    public void writeObject(Object o) {
        try {
            out.writeObject(o);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void writeGrouped(Map<DataFrame.ValueGroup,DataFrame> merged) throws IOException {
        out.writeObject(merged);
    }

    public Map<DataFrame.ValueGroup,DataFrame> readMap() throws IOException {
        try {
            return (Map<DataFrame.ValueGroup, DataFrame>) in.readObject();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            throw new IllegalStateException("no groups recieved");
        }
    }
}
