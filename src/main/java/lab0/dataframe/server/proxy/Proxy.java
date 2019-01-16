package lab0.dataframe.server.proxy;

import lab0.dataframe.server.protocol.PortType;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;

public class Proxy {

    static class NodeConnections implements Runnable {
        BlockingQueue<WorkerManager> managers;
        ExecutorService exec;

        public NodeConnections(BlockingQueue<WorkerManager> managers, ExecutorService exec) {
            this.managers = managers;
            this.exec = exec;
        }

        @Override
        public void run() {
            try {
                ServerSocket sock = new ServerSocket(PortType.WORKER.getPort());
                while (!Thread.interrupted())
                    exec.submit(new WorkerManager(sock.accept(), managers));
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }

    private static class ClientConnections implements Runnable {
        final BlockingQueue<WorkerManager> managers;
        ExecutorService exec;

        public ClientConnections(BlockingQueue<WorkerManager> managers, ExecutorService exec) {
            this.managers = managers;
            this.exec = exec;
        }

        @Override
        public void run() {
            try {
                ServerSocket sock = new ServerSocket(PortType.CLIENT.getPort(),2);
                try {

                    synchronized (managers) {
                    while (managers.size() <= 0) {
                            managers.wait(1000);

                    }
                }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                while (!Thread.interrupted())
                    exec.submit(new ClientManager(sock.accept(), managers, exec));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String... args) {

        ExecutorService exec =Executors.newFixedThreadPool(2);
        PriorityBlockingQueue<WorkerManager> managers = new PriorityBlockingQueue<>();

        exec.submit(new NodeConnections(managers, Executors.newWorkStealingPool(3)));
        exec.submit(new ClientConnections(managers,  Executors.newWorkStealingPool(3)));

        while (!exec.isShutdown()) {
            try {
                Thread.sleep(1000 * 120);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


}
