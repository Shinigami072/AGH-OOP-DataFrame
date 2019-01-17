package lab0.dataframe.server.proxy;

import lab0.dataframe.DataFrame;
import lab0.dataframe.server.protocol.Task;
import lab0.dataframe.server.protocol.WorkerCommType;
import lab0.dataframe.server.protocol.WorkerProtocolParser;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.Arrays;
import java.util.concurrent.BlockingQueue;

class WorkerManager implements Comparable<WorkerManager>, Runnable {
    private final Socket communicator;
    private final BlockingQueue<WorkerManager> available_workers;
    private final WorkerProtocolParser parser;


    private WorkerStatus status;
    private Task current;
    private int cores;
    private Object[] arguments;
    private Object[] result;

    WorkerManager(Socket communicator, BlockingQueue<WorkerManager> workers) throws IOException {
        this.communicator = communicator;
        this.status = WorkerStatus.CONNECTING;
        this.available_workers = workers;
        this.current = Task.NONE;
        parser = new WorkerProtocolParser(
                new ObjectInputStream(communicator.getInputStream()),
                new ObjectOutputStream(communicator.getOutputStream()));

        if (parser.readType() == WorkerCommType.ANNOUNCE) {
            cores = parser.readInt();
            this.status = WorkerStatus.IDLE;
        } else {
            throw new IllegalStateException("wrong protocol");
        }


    }

    @Override
    public int compareTo(WorkerManager workerManager) {
        return cores - workerManager.cores;
    }


    @Override
    public String toString() {
        return "Worker[" + status.toString() + "](" + communicator.getInetAddress().toString() + ")" + Thread.currentThread().getId();
    }

    @Override
    public void run() {
        try {

            while (status != WorkerStatus.OFFLINE) {
                updateStatus();

                switch (status) {

                    case IDLE:
                        synchronized (this) {
                            this.wait(500);
                        }
                        break;//Await Task

                    case CONNECTING:
                        status = WorkerStatus.IDLE;
                        break;//empty branch

                    case REQUESTING: //request computation.
                        parser.shedulePacket(current, arguments);
                        status = WorkerStatus.WORKING;
                        break;

                    case WORKING: //await computation completion.
                        WorkerCommType t = parser.readType();//read result

                        switch (t) {

                            case TASK_SCHEDULED:
                            case ANNOUNCE:
                                break;//empty branch todo: this or not

                            case HEARTBEAT:
                                break;//todo: hearbeat packets
                            case DISCONNECT:
                                status = WorkerStatus.OFFLINE;
                                break;//todo: gracefull disconnection

                            case TASK_COMPLETED:
                                result = parser.readObjects();
                                parser.reset();
                                status = WorkerStatus.IDLE;
                                current = Task.NONE;
                                synchronized (this) {
                                    this.notifyAll();
                                }
                                break;

                            case TASK_REJECTED:
                                status = WorkerStatus.IDLE;//todo: graceful rejection
                                current = Task.REJECTED;
                                parser.reset();
                                synchronized (this) {
                                    this.notifyAll();
                                }
                                break;

                        }

                        break;
                }
            }

        } catch (IOException | ClassNotFoundException | InterruptedException e) {

            e.printStackTrace();
            System.err.println(this + " died unexpectedly");

        } finally {
            try {
                if (status != WorkerStatus.OFFLINE) {
                    parser.disconnectPacket();
                    available_workers.remove(this);
                }
                communicator.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void updateStatus() {
        synchronized (this) {
            if (current == Task.NONE && status != WorkerStatus.IDLE && status != WorkerStatus.CONNECTING && status != WorkerStatus.OFFLINE)
                status = WorkerStatus.IDLE;

            if (!available_workers.contains(this) && status == WorkerStatus.IDLE) {
                current = Task.NONE;

                this.notifyAll();
                try {
                    available_workers.put(this);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        if (Thread.interrupted()) {
            status = WorkerStatus.OFFLINE;
        }

    }

    public Object[] request(Task type, Object... arguments) throws InterruptedException {
        synchronized (this) {
            while (status != WorkerStatus.IDLE && current == Task.NONE) {
                if (status != WorkerStatus.WORKING && status != WorkerStatus.REQUESTING) {
                    this.notifyAll();
                    throw new InterruptedException();
                }
                System.out.println(this + "request retry");
                this.wait();
            }

            status = WorkerStatus.REQUESTING;
            current = type;

            this.arguments = arguments;

            System.out.println(this + "request recieved " + type + ":");
            this.notify();


            while (current != Task.NONE) {
                System.out.print(this + "request results wait");
                for (Object o : arguments) {
                    if (o instanceof DataFrame)
                        System.out.print(Arrays.toString(((DataFrame) o).getNames()));
                    else if (o instanceof Object[])
                        System.out.println(Arrays.toString((Object[]) o));
                    else
                        System.out.print(o);
                    System.out.print(" ");
                }
                System.out.println();
                this.wait();
            }

            if (current == Task.REJECTED) {
                current = Task.NONE;
                throw new InterruptedException();
            }
            System.out.println(this + "request results recieved");

            return result;

        }
    }
}