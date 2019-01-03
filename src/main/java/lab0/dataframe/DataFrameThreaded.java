package lab0.dataframe;

import lab0.dataframe.exceptions.*;
import lab0.dataframe.groupby.Applyable;
import lab0.dataframe.groupby.GroupBy;
import lab0.dataframe.values.Value;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class DataFrameThreaded extends DataFrame {
    private final ExecutorService executorService;

    public DataFrameThreaded(ExecutorService executorService, DataFrame df) {
        this(executorService, df.getNames(), df.getTypes());

        try {
            ArrayList<Value[]> rows = new ArrayList<>(df.size());

            for (int i = 0; i < df.size(); i++) {
                rows.add(df.getRecord(i));
            }

            addAllRecords(rows);
        } catch (DFColumnTypeException ignore) {
        }

    }

    public DataFrameThreaded(ExecutorService executorService, String[] nazwyKolumn, Class<? extends Value>[] typyKolumn) {
        super(nazwyKolumn, typyKolumn);
        this.executorService = executorService;
    }

    public DataFrameThreaded(ExecutorService executorService, String path, Class<? extends Value>[] column_type) throws IOException, DFColumnTypeException, DFValueBuildException {
        this(executorService, path, column_type, null);
    }

    public DataFrameThreaded(ExecutorService executorService, String path, Class<? extends Value>[] column_type, String[] column_name) throws IOException, DFColumnTypeException, DFValueBuildException {
        super(column_type.length);
        this.executorService = executorService;

        boolean header = column_name == null;

        for (int i = 0; i < column_type.length; i++)
            columns[i] = new Column(header ? "" : column_name[i], column_type[i]);

        readFile(path, header);
    }

//    @Override
//    protected void loadData(BufferedReader br) throws IOException, DFValueBuildException {
//
//        int colCount = getColCount();
//
//        //Builder threads setup
//        List<Future<Boolean>> builders = new ArrayList<>(colCount);
//        List<AtomicBoolean> isDone = new ArrayList<>(colCount);
//
//        AtomicBoolean finished = new AtomicBoolean(false);
//
//
//        //Builder Thread IO
//        List<Queue<String>> sources = new ArrayList<>(colCount);
//        List<Queue<Value>> values = new ArrayList<>(colCount);
//
//        for (int i = 0; i < colCount; i++) {
//            int Fi = i;
//
//            isDone.add(new AtomicBoolean(false));
//            sources.add(new LinkedList<>());
//            values.add(new LinkedList<>());
//
//            builders.add(executorService.submit(new Callable<Boolean>() {
//                @Override
//                public Boolean call() throws DFValueBuildException {
//
//                    Value.ValueBuilder factory = Value.builder(getTypes()[Fi]);
//                    Queue<String> source = sources.get(Fi);
//                    Queue<Value> value = values.get(Fi);
//
//                    outer:
//                    do {
//
//                        Value v;
//
//                        synchronized (sources.get(Fi)) {
//
//
//                            //wait until Avaliable
//                            while (source.size() <= 0) {
//                                if (finished.get())
//                                    break outer;
//                                else {
//                                    try {
//                                        source.wait();
//                                    } catch (InterruptedException e) {
//                                        e.printStackTrace();
//                                    }
//                                }
//                            }
//
//                            //build Value
//                            v = factory.build(source.poll());
//
//                        }
//
//                        //send to Adder and Notify
//                        synchronized (values.get(Fi)) {
//                            value.offer(v);
//                            values.get(Fi).notify();
//                        }
//
//                        //repeat unitl everything is loaded
//                    } while (!finished.get() || source.size() > 0);
//
//
//                    //eventually tell them - im done
//                    isDone.get(Fi).lazySet(true);
//                    //and notify them
//                    synchronized (values.get(Fi)) {
//                        values.get(Fi).notifyAll();
//                    }
//
//                    return null;
//                }
//            }));
//        }
//
//
//        Future<?> Merger = executorService.submit(new Runnable() {
//            boolean isFinished() {
//
//                //if any Queue still has values
//                for (Queue<Value> q : values)
//                    if (q.size() > 0)
//                        return false;
//
//                //if any builder is still building
//                for (AtomicBoolean b : isDone) {
//                    if (!b.get())
//                        return false;
//                }
//
//                return true;
//            }
//
//            boolean isFinished(int i) {
//                return values.get(i).size() <= 0 && isDone.get(i).get();
//            }
//
//            @Override
//            public void run() {
//                //reuse the same Array for every additinon
//                Value[] row = new Value[colCount];
//
//                outer:
//                do {
//
//                    //Get One of each value
//                    for (int i = 0; i < values.size(); i++)
//
//                        synchronized (values.get(i)) {
//
//                            while ((row[i] = values.get(i).poll()) == null) {
//
//                                //if there will be no more values - stop adding
//                                if (isFinished(i))
//                                    break outer;
//
//                                    //if there is something to be added
//                                else if (values.get(i).size() > 0)
//                                    continue;
//
//                                //wait until help arrives
//                                try {
//                                    values.get(i).wait();
//                                } catch (InterruptedException e) {
//                                    e.printStackTrace();
//                                }
//                            }
//                        }
//
//                    //is should have correct Types and ammounts
//                    try {
//                        addRecord(row);
//                    } catch (DFColumnTypeException e) {
//                        e.printStackTrace();
//                    }
//
//                } while (!isFinished());
//            }
//
//
//        });
//
//
//        //Load From File into Queues
//        String[] strLine;
//        String temp;
//        int row = 0;
//        while ((temp = br.readLine()) != null) {
//
//            strLine = temp.split(",");
//
//            if (strLine.length != colCount)
//                throw new DFDimensionException("Wrong row size in foile at row" + row);
//
//            row++;
//            //send string to each builder and notify him
//            for (int i = 0; i < strLine.length; i++) {
//                String s = strLine[i];
//                synchronized (sources.get(i)) {
//
//                    sources.get(i).offer(s);
//                    sources.get(i).notify();
//                }
//            }
//        }
//
//        try {
//            //notify of completion
//            finished.set(true);
//            for (int i = 0; i < sources.size(); i++) {
//                synchronized (sources.get(i)) {
//                    sources.get(i).notifyAll();
//                }
//            }
//
//            //in case there is an exception
//            for (Future<Boolean> builder : builders) {
//                builder.get();
//            }
//
//            //make sure it completed succesfully
//            Merger.get();
//
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//            throw new DFValueBuildException("Multi threader Int:" + e.getMessage());
//        } catch (ExecutionException e) {
//            e.printStackTrace();
//            throw new DFValueBuildException(e.getCause().getMessage());
//        }
//
//    }


//    @Override
//    public void addRecord(Value... values) throws DFColumnTypeException, DFDimensionException {
//        synchronized (lazyaddingQueue){
//        lazyaddingQueue.add(values);
//        if(lazyaddingQueue.size() > 10000) {
//            addAllRecords(lazyaddingQueue);
//            lazyaddingQueue.clear();
//        }
//        }
//    }

    public static DataFrame convertCallableToDataFrame(List<Callable<Column>> callables, ExecutorService executorService) {
        try {
            List<Future<Column>> isComplete = executorService.invokeAll(callables);

            //copy the columns into array
            Column[] targetColumns = new Column[isComplete.size()];
            for (int i = 0; i < isComplete.size(); i++) {
                targetColumns[i] = isComplete.get(i).get();
            }

            return new DataFrame(targetColumns);

        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

//    @Override
//    protected void loadData(BufferedReader br) throws IOException, DFValueBuildException {
//        String temp;
//        String[] strLine;
//
//        BlockingQueue<String[]> processingQueue = new LinkedBlockingQueue<>();
//
//        Future<Object> builder = executorService.submit(new Callable<Object>() {
//            @Override
//            public Object call() throws DFColumnTypeException, InterruptedException {
//
//                LinkedList<Value[]> values = new LinkedList<>();
//
//                Value.ValueBuilder[] builders = new Value.ValueBuilder[columns.length];
//                for (int i = 0; i < columns.length; i++) {
//                    builders[i] = Value.builder(columns[i].typ);
//                }
//                AtomicBoolean lastLock = null;
//                int RowNum = 0;
//                do {
//                    String[] strings = processingQueue.take();
//
//                    if (strings.length == 0)
//                        break;
//
//                    Value[] tempValues = new Value[columns.length];
////                    List<Callable<Value>> list = new ArrayList<>(strings.length);
//                    for (int i = 0; i < strings.length; i++) {
//                        int Fi = i;
////                        list.add(new Callable<Value>() {
////                            @Override
////                            public Value call() throws Exception {
//                        try {
//                            tempValues[Fi] = builders[Fi].build(strings[Fi]);
//                        } catch (DFValueBuildException e) {
//                            e.printStackTrace();
//                        }
////                                return tempValues[Fi];
////                            }
////                        });
//                    }
////                    executorService.invokeAll(list);
//                    values.add(tempValues);
//                    RowNum++;
////                    addRecord(tempValues);
//
//                } while (true);
////                catch (DFValueBuildException e) {
////                    e.printStackTrace();
////                }
//
//                addAllRecords(values);
//                return null;
//            }
//        });
//
//
//        try {
//            int i = 0;
//            while ((temp = br.readLine()) != null) {
//                strLine = temp.split(",");
//                if (strLine.length == columns.length)
//                    processingQueue.put(strLine);
//                else
//                    continue;
//                i++;
//            }
//            processingQueue.put(new String[0]);
//        } catch (InterruptedException e) {
//            builder.cancel(true);
//            e.printStackTrace();
//        }
//
//        try {
//            builder.get();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        } catch (ExecutionException e) {
//            e.printStackTrace();
//        }
//    }

    @Override
    public DataFrame get(String[] cols, boolean copy) throws DFZeroLengthCreationException, CloneNotSupportedException {
        if (!copy || cols.length < 2)
            return super.get(cols, copy);


        List<Callable<Column>> callables = new ArrayList<>(getColCount());
        for (int i = 0; i < cols.length; i++) {
            int Fi = i;
            callables.add(new Callable<Column>() {
                @Override
                public Column call() throws CloneNotSupportedException {
                    return get(cols[Fi]).copy();
                }
            });
        }

        return convertCallableToDataFrame(callables, executorService);
    }

//    @Override
//    public Value[] getRecord(int index) {
//        if (index < 0 || index > size())
//            throw new DFIndexOutOfBounds("Out of bounds: " + index);
//
//
//        Value[] row = new Value[getColCount()];
//
//        //split search into different threads
//        List<Callable<Value>> callables = new ArrayList<>(getColCount());
//        for (int i = 0; i < getColCount(); i++) {
//            int Fi = i;
//            callables.add(new Callable<Value>() {
//                @Override
//                public Value call() throws DFColumnTypeException {
//                    return columns[Fi].get(index);
//                }
//            });
//        }
//
//
//        //unwrapping Exceptions
//        try {
//
//            List<Future<Value>> values = new ArrayList<>(callables.size());
//            for (int i = 0; i < callables.size(); i++) {
//                values.add(executorService.submit(callables.get(i)));
//            }
//            for (int i = 0; i < getColCount(); i++) {
//                row[i] = values.get(i).get();
//            }
//
//            return row;
//        } catch (ExecutionException | InterruptedException e) {
//            throw new DFIndexOutOfBounds(e.getMessage());
//        }
//    }

    public void addAllRecords(Collection<Value[]> rows) throws DFColumnTypeException, DFDimensionException {

        List<Callable<Integer>> callables = new ArrayList<>(getColCount());

        //cache values
        int colCount = getColCount();
        int size = size();

        for (int i = 0; i < colCount; i++) {
            int Fi = i;
            callables.add(new Callable<Integer>() {
                @Override
                public Integer call() throws DFColumnTypeException, DFDimensionException {

                    //exception handling
                    int i = 0;
                    Iterator<Value[]> it = rows.iterator();
                    for (Value[] row : rows) {

                        if (row.length != colCount) {
                            throw new DFDimensionException("Row " + (size + i) + "mismatched length");
                        }
                        i++;

                        columns[Fi].add(row[Fi]);
                    }
                    return rows.size();
                }
            });
        }

        try {
            List<Future<Integer>> futures = executorService.invokeAll(callables);

            for (Future<Integer> f : futures) {
                f.get();
            }

            rowNumber += rows.size();

        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            Throwable t = e.getCause();
            if (t instanceof DFDimensionException)
                throw (DFDimensionException) t;
            else if (t instanceof DFColumnTypeException)
                throw (DFColumnTypeException) t;
            else
                e.printStackTrace();

        }
    }

    //todo: interrupts everywhere
    @Override
    public DataFrame iloc(int from, int to) throws DFColumnTypeException {
        if (from - to < 500)
            return super.iloc(from, to);

        checkBounds(from, to);

        //copy columns in parallel
        List<Callable<Column>> callables = new ArrayList<>(getColCount());
        for (int i = 0; i < getColCount(); i++) {
            int Fi = i;

            callables.add(new Callable<Column>() {
                @Override
                public Column call() throws DFColumnTypeException {

                    Column c = new Column(columns[Fi].getName(), columns[Fi].getType());
                    for (int j = from; j < to; j++) {
                        c.add(columns[Fi].get(j));
                    }

                    return c;
                }
            });
        }

        return convertCallableToDataFrame(callables, executorService);
    }

    @Override
    public GroupBy groupBy(String... colname) {

        Map<ValueGroup, DataFrame> storage = new ConcurrentHashMap<>();
//        Map<ValueGroup, List<Integer>> storage = new ConcurrentHashMap<>();
        DataFrame keys = null;

        try {
            keys = get(colname, false);
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }

        DataFrame finalKeys = keys;
        class Classifier implements Callable<Object> {

            final int start;
            final int end;

            Classifier(int begin, int end) {
                this.start = begin;
                this.end = end;
            }

            @Override
            public Object call() throws InterruptedException, DFColumnTypeException {
                if (end - start > 100) {
                    int mid = (start + end) / 2;
                    executorService.invokeAll(Arrays.asList(new Classifier(start, mid), new Classifier(mid, end)));
                    return null;
                } else {
                    for (int i = start; i < end; i++) {
//                    assert finalKeys != null;

                        //simplify to one getRecord?
                        ValueGroup key = new ValueGroup(finalKeys.getRecord(i));
                        Value[] row = getRecord(i);

                        //todo: dataframe view
                        DataFrame group = storage.computeIfAbsent(key, k -> new DataFrameSparse(getNames(), row));
//                        List<Integer> group = storage.computeIfAbsent(key, k -> new ArrayList<>(end - start));
//                        try {
                        synchronized (group) {
//                            if (row.length > 0) {
//                                group.add(i);
//                            }
//                                getRecord(i);
                                group.addRecord(row);
                        }
//                        } catch (DFColumnTypeException e) {
//                            e.printStackTrace();
//                        }
//                    Queue<Integer> queue = builderQueues.computeIfAbsent(key, k -> new LinkedList<>());
//                    queue.offer(i);

                    }
                    return null;
                }
            }
        }

        try {
            int size = size();
//            int wokrSize = 25_000;
//            int mid=size/2;
//            int halfMid=mid/2;
//            executorService.invokeAll(Arrays.asList(new Classifier(0, halfMid),new Classifier(halfMid, mid),new Classifier(mid,mid+halfMid),new Classifier(mid+halfMid,size)));
//            List<Classifier> workers = new ArrayList<>(size / wokrSize);
//            for (int i = 0; i < size; i += wokrSize)
//                workers.add(new Classifier(i, Math.min(i + wokrSize, size)));
//            executorService.invokeAll(workers);
            executorService.invokeAll(Collections.singletonList(new Classifier(0, size)));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
//        return new GrupatorThreaded(storage, colname, keys.getTypes());//todo - czech perfofmans pls

    }

    class GrupatorThreaded implements GroupBy {

        Map<ValueGroup, DataFrame> groups;
        String[] datanames;

        String[] keynames;
        Class<? extends Value>[] keyTypes;

        GrupatorThreaded(Map<ValueGroup, DataFrame> groups, String[] keynames, Class<? extends Value>[] keyTypes) {
            this.groups = groups;
            this.keynames = keynames;
            this.keyTypes = keyTypes;

//            Set<String> datanames = new HashSet<String>(Arrays.asList(getNames()));
//            datanames.removeAll(Arrays.asList(keynames));
//
//            this.datanames = datanames.toArray(new String[0]);
        }

        @Override
        public DataFrame apply(Applyable apply) throws DFApplyableException {
            List<Callable<DataFrame>> groupCalculator = new ArrayList<>(groups.size());
            for (ValueGroup key : groups.keySet()) {
                groupCalculator.add(new Callable<DataFrame>() {

                    @Override
                    public DataFrame call() throws DFApplyableException, CloneNotSupportedException {

                        DataFrame group = groups.get(key);
                        DataFrame cutDown = group.get(datanames, false);

                        return apply.apply(cutDown);


                    }
                });
            }


            try {
                List<Future<DataFrame>> calculated = executorService.invokeAll(groupCalculator);
                SortedSet<ValueGroup> k = new TreeSet<>(groups.keySet());
                Iterator<ValueGroup> keys = k.iterator();
                DataFrame output = null;

                for (Future<DataFrame> f : calculated) {
                    ValueGroup key = keys.next();
                    DataFrame group = f.get();
                    if (output == null)
                        output = GroupBy.getOutputDataFrame(keyTypes, keynames, group.getTypes(), group.getNames());

                    GroupBy.addGroup(output, key.getId(), group);
                }
                return output;

            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (DFColumnTypeException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }

            return null;
        }

    }

}
