package lab0.dataframe.server.proxy;

import lab0.dataframe.DataFrame;
import lab0.dataframe.exceptions.DFColumnTypeException;
import lab0.dataframe.groupby.GroupBy;
import lab0.dataframe.server.protocol.ApplyOperation;
import lab0.dataframe.server.protocol.ClientProtocolParser;
import lab0.dataframe.server.protocol.Task;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;

public class ClientManager implements Runnable {
    private final Socket comm;
    private BlockingQueue<WorkerManager> managers;
    private ExecutorService exec;
    private ClientProtocolParser parser;
    private Map<Integer, DataFrame> DF;
    private Map<Integer, Map<String, Map<DataFrame.ValueGroup, DataFrame>>> GDF;

    public ClientManager(Socket comm, BlockingQueue<WorkerManager> managers, ExecutorService exec) throws IOException {
        System.out.println(comm);
        this.comm = comm;
        this.managers = managers;
        this.exec = exec;
        ObjectOutputStream out = new ObjectOutputStream(comm.getOutputStream());
        ObjectInputStream in = new ObjectInputStream(comm.getInputStream());
        this.parser = new ClientProtocolParser(in, out);
        DF = new HashMap<>();
        GDF = new HashMap<>();
    }

    private int loadDF() throws IOException {
        int HashDF = parser.readInt();

        boolean contains = DF.containsKey(HashDF);
        parser.writeBoolean(contains);
        parser.flush();

        if (!contains) {
            DF.put(HashDF, parser.readDataFrame());
            GDF.put(HashDF, new HashMap<>());
        }

        return HashDF;
    }

    private DataFrame apply(int hashDF, String key, String[] colnames, ApplyOperation apply) throws InterruptedException {
        DataFrame output = null;

        Map<DataFrame.ValueGroup, DataFrame> grouped = GDF.get(hashDF).get(key);
        SortedSet<DataFrame.ValueGroup> sortedKeys = new TreeSet<>(grouped.keySet());

        Set<String> allnames = new HashSet<>(Arrays.asList(grouped.values().iterator().next().getNames()));
        allnames.removeAll(Arrays.asList(colnames));

        String[] datanames = allnames.toArray(new String[0]);

        class ApplyableWorker implements Callable<DataFrame> {

            private DataFrame group;

            private ApplyableWorker(DataFrame group) {
                this.group = group;
            }

            @Override
            public DataFrame call() throws Exception {
                return ((DataFrame) managers.take().request(Task.APPLY, apply, group.get(datanames, false))[0]);
            }
        }

        List<ApplyableWorker> workers = new ArrayList<>();
        for (DataFrame.ValueGroup group : sortedKeys) {
            workers.add(new ApplyableWorker(grouped.get(group)));
        }


        List<Future<DataFrame>> futures = exec.invokeAll(workers);
        try {

            Iterator<DataFrame.ValueGroup> keyInt = sortedKeys.iterator();
            for (Future<DataFrame> dataFrameFuture : futures) {

                DataFrame df = dataFrameFuture.get();
                DataFrame.ValueGroup k = keyInt.next();
                System.out.println(this + " merging:" + k + " " + df.size());
                if (output == null) {
                    System.out.println("ouptput creation");
                    output = GroupBy.getOutputDataFrame(k.getTypes(), colnames, df.getTypes(), df.getNames());
                    System.out.println("ouptput created");
                }
                System.out.println("ouptput adding");
                GroupBy.addGroup(output, k.getId(), df);
                System.out.println("ouptput added");
            }

        } catch (ExecutionException | DFColumnTypeException e) {
            e.printStackTrace();
            throw new InterruptedException("");
        }
        return output;
    }


    private void group(int HashDF, String key, String[] colnames) throws InterruptedException {
        boolean contains = GDF.get(HashDF).containsKey(key);


        if (!contains) {
            System.out.println("calculating Groups");

            DataFrame df = DF.get(HashDF);
            GDF.get(HashDF).put(key, (Map<DataFrame.ValueGroup, DataFrame>) managers.take().request(Task.GROUP, colnames, df)[0]);
        }
    }

    @Override
    public void run() {
        try {

            outer:
            while (true) {
                switch (parser.readRequestType()) {
                    case APPLY:
                        ApplyOperation apply = parser.readApplyType();

                        DataFrame response = null;
                        try {
                            response = ((DataFrame) managers.take().request(Task.APPLY, apply, DF.get(loadDF()))[0]);
                            System.out.println(this + "sending");
                            parser.writeDataFrame(response);
                        } catch (InterruptedException e) {
                            System.out.println(this+"unexpected error");
                            parser.writeObject(null);
                            e.printStackTrace();
                        }

                        parser.flush();
                        System.out.println(this + "sent");

                        break;

                    case GROUP:
                        String[] colnames = parser.readColnames();
                        String key = Arrays.toString(colnames);

                        int HashDF = loadDF();

                        try {
                            group(HashDF, key, colnames);
                            System.out.println(this + "sending");
                            parser.writeGrouped(GDF.get(HashDF).get(key));
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                            System.out.println(this+"unexpected error");
                            parser.writeObject(null);
                        }
                        parser.flush();
                        System.out.println(this + "sent");

                        break;
                    case APPLY_GROUP:
                        apply = parser.readApplyType();
                        colnames = parser.readColnames();
                        key = Arrays.toString(colnames);

                        HashDF = loadDF();

                        try {

                            System.out.println(this + "grouping");
                        group(HashDF, key, colnames);

                        System.out.println(this + "applying");
                        DataFrame result = apply(HashDF, key, colnames, apply);

                        System.out.println(this + "sending");
                        parser.writeDataFrame(result);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                            System.out.println(this+"unexpected error");
                            parser.writeObject(null);
                        }
                        parser.flush();
                        System.out.println(this + "sent");

                        break;
                    case DISCONNECT:
                        break outer;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

//    private Map<DataFrame.ValueGroup, DataFrame> merge(List<Future<Map<DataFrame.ValueGroup, DataFrame>>> responses) throws ExecutionException, InterruptedException {
//
//        Map<DataFrame.ValueGroup, DataFrame> map = new HashMap<>();
//
//        for (Future<Map<DataFrame.ValueGroup, DataFrame>> response : responses) {
//            for (DataFrame.ValueGroup k : response.get().keySet()) {
//                DataFrame df = response.get().get(k);
//                map.compute(k, (key, value) -> (value == null) ? df : addDF(value, df));
//            }
//        }
//
//        for (DataFrame.ValueGroup key : map.keySet()) {
//            DataFrame df = map.get(key);
//            if (df instanceof DataFrameSparse) {
//                ((DataFrameSparse) df).optimizeStorage();
//            }
//        }
//
//        return map;
//    }

//    private DataFrame addDF(DataFrame value, DataFrame add) {
//        for (int i = 0; i < add.size(); i++) {
//            try {
//                value.addRecord(add.getRecord(i));
//            } catch (DFColumnTypeException e) {
//                e.printStackTrace();
//            }
//        }
//        return value;
//    }

    @Override
    public String toString() {
        return "Client[" + comm + "]";
    }
}
