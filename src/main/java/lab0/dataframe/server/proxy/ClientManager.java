package lab0.dataframe.server.proxy;

import lab0.dataframe.DataFrame;
import lab0.dataframe.DataFrameSparse;
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

import static java.lang.Math.min;

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

    int loadDF() throws IOException {
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

    private DataFrame apply(int hashDF, String key, String[] colnames, ApplyOperation apply) {
        DataFrame output=null;

        Map<DataFrame.ValueGroup, DataFrame> grouped = GDF.get(hashDF).get(key);
        SortedSet<DataFrame.ValueGroup> sortedKeys = new TreeSet<>(grouped.keySet());

        Set<String> allnames = new HashSet<String>(Arrays.asList(grouped.values().iterator().next().getNames()));
        allnames.removeAll(Arrays.asList(colnames));

        String[] datanames = allnames.toArray(new String[0]);

        class ApplyableWorker implements Callable<DataFrame> {

            DataFrame group;

            public ApplyableWorker(DataFrame group) {
                this.group = group;
            }

            @Override
            public DataFrame call() throws Exception {
                return ((DataFrame) managers.take().request(Task.APPLY, apply, group.get(datanames,false))[0]);
            }
        }

        List<ApplyableWorker> workers = new ArrayList<>();
        for (DataFrame.ValueGroup group : sortedKeys) {
            workers.add(new ApplyableWorker(grouped.get(group)));
        }

        try {
            List<Future<DataFrame>> futures=exec.invokeAll(workers);

            Iterator<DataFrame.ValueGroup> keyInt = sortedKeys.iterator();
            for (Future<DataFrame> dataFrameFuture:futures) {

                DataFrame df = dataFrameFuture.get();
                DataFrame.ValueGroup k = keyInt.next();
                System.out.println(k);
                System.out.println(df);

                if(output==null)
                    output=GroupBy.getOutputDataFrame(k.getTypes(),colnames,df.getTypes(),datanames);

                GroupBy.addGroup(output,k.getId(),df);
            }
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        } catch (DFColumnTypeException e) {
            e.printStackTrace();
        }
        return output;
    }


    void group(int HashDF, String key, String[] colnames) throws InterruptedException {
        boolean contains = GDF.get(HashDF).containsKey(key);


        if (!contains) {
            System.out.println("splitting");
            try {

                DataFrame df = DF.get(HashDF);
                ArrayList<WorkerManager> avaliable_workers = new ArrayList<>();

                int N = 1;
                avaliable_workers.add(managers.take());
                int size_T = df.size() / N;

                class GrouperWorker implements Callable<Map<DataFrame.ValueGroup, DataFrame>> {
                    WorkerManager worker;
                    DataFrame df;
                    private int start;
                    private int end;

                    public GrouperWorker(WorkerManager worker, DataFrame df, int start, int end) {
                        System.out.println("partition" + worker + " " + start + "-" + end);
                        this.worker = worker;
                        this.df = df;
                        this.start = start;
                        this.end = end;
                    }

                    @Override
                    public Map<DataFrame.ValueGroup, DataFrame> call() throws Exception {
                        System.out.println(start + ":" + end);
                        return (Map<DataFrame.ValueGroup, DataFrame>) worker.request(Task.GROUP, colnames, df.iloc(start, end))[0];
                    }
                }

                ArrayList<GrouperWorker> responses = new ArrayList<>();
                Iterator<WorkerManager> worker = avaliable_workers.iterator();

                for (int i = 0; i < df.size() - 1; i += size_T) {
                    responses.add(new GrouperWorker(worker.next(), df, i, min(i + size_T - 1, df.size() - 1)));
                }
                List<Future<Map<DataFrame.ValueGroup, DataFrame>>> resp = exec.invokeAll(responses);
                GDF.get(HashDF).put(key, merge(resp));

            } catch (ExecutionException e) {
                e.printStackTrace();
            }
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

                        DataFrame response = ((DataFrame) managers.take().request(Task.APPLY, apply, DF.get(loadDF()))[0]);
                        parser.writeDataFrame(response);
                        parser.flush();

                        break;

                    case GROUP:
                        String[] colnames = parser.readColnames();
                        String key = Arrays.toString(colnames);

                        int HashDF = loadDF();

                        group(HashDF, key, colnames);

                        System.out.println("reults");
                        parser.writeGrouped(GDF.get(HashDF).get(key));
                        parser.flush();

                        break;
                    case APPLY_GROUP:
                        apply = parser.readApplyType();
                        colnames = parser.readColnames();
                        key = Arrays.toString(colnames);

                        HashDF = loadDF();

                        group(HashDF, key, colnames);
                        DataFrame result = apply(HashDF, key, colnames, apply);
                        parser.writeDataFrame(result);
                        parser.flush();
                        break;
                    case DISCONNECT:
                        break outer;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    private Map<DataFrame.ValueGroup, DataFrame> merge(List<Future<Map<DataFrame.ValueGroup, DataFrame>>> responses) throws ExecutionException, InterruptedException {

        Map<DataFrame.ValueGroup, DataFrame> map = new HashMap<>();

        for (Future<Map<DataFrame.ValueGroup, DataFrame>> response : responses) {
            for (DataFrame.ValueGroup k : response.get().keySet()) {
                DataFrame df = response.get().get(k);
                map.compute(k, (key, value) -> (value == null) ? df : addDF(value, df));
            }
        }

        for (DataFrame.ValueGroup key : map.keySet()) {
            DataFrame df = map.get(key);
            if (df instanceof DataFrameSparse) {
                ((DataFrameSparse) df).optimizeStorage();
            }
        }

        return map;
    }

    private DataFrame addDF(DataFrame value, DataFrame add) {
        for (int i = 0; i < add.size(); i++) {
            try {
                value.addRecord(add.getRecord(i));
            } catch (DFColumnTypeException e) {
                e.printStackTrace();
            }
        }
        return value;
    }
}
