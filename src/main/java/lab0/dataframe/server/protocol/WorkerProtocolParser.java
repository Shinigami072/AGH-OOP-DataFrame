package lab0.dataframe.server.protocol;

import lab0.dataframe.DataFrame;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class WorkerProtocolParser {
    public WorkerProtocolParser(ObjectInputStream in, ObjectOutputStream out) {
        this.in = in;
        this.out = out;
    }

    private ObjectInputStream in;
    private ObjectOutputStream out;

    public void announcePacket(int workers) throws IOException {
        out.writeObject(WorkerCommType.ANNOUNCE);
        out.writeInt(workers);
        out.flush();
    }

    public void disconnectPacket() throws IOException {
        out.writeObject(WorkerCommType.DISCONNECT);
        out.flush();
    }

    public void shedulePacket(Task current, Object[] arguments) throws IOException {
        out.writeObject(WorkerCommType.TASK_SCHEDULED);
        out.writeObject(current);
        if (arguments.length > 0)
            writeObjects(arguments);
    }

    public void completePacket(Object... results) throws IOException {
        out.writeObject(WorkerCommType.TASK_COMPLETED);
        writeObjects(results);
        out.flush();
    }

    public void rejectPacket() throws IOException {
        out.writeObject(WorkerCommType.TASK_REJECTED);
        out.flush();
    }

    public WorkerCommType readType() throws IOException {
        try {
            return (WorkerCommType) in.readObject();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            throw new IllegalStateException("commType not recieved");
        }
    }

    public Task readTask() throws IOException {
        try {
            return (Task) in.readObject();
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("task not recieved");
        }
    }

    public ApplyOperation readApplyOperation() throws IOException {
        try {
            return (ApplyOperation) in.readObject();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            throw new IllegalStateException("ApplyOperation not recieved");
        }
    }

    public String[] readColnames() throws IOException {
        try {
            return (String[]) (in.readObject());
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("colnames not recieved");
        }
    }

    public void writeObjects(Object... results) throws IOException {
        out.writeInt(results.length);
        for (Object o : results) {
            out.writeObject(o);
        }
    }

    public Object[] readObjects() throws IOException, ClassNotFoundException {
        int len = in.readInt();

        Object[] results = new Object[len];

        for (int i = 0; i < len; i++) {
            results[i] = in.readObject();
        }

        return results;
    }



    public DataFrame writeDataFrame() throws IOException {
        try {
            return (DataFrame) (in.readObject());
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("dataFrame not recieved");
        }
    }

    public void readDataFrame(DataFrame df) throws IOException {
        out.writeObject(df);
    }
    public void writeInt(int val) throws IOException {
         out.writeInt(val);
    }

    public int readInt() throws IOException {
        return in.readInt();
    }

}
