package lab0.dataframe;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DefultSingletons {
    private static DefultSingletons ourInstance = new DefultSingletons();

    public static DefultSingletons getInstance() {
        return ourInstance;
    }
    public static final int coreCount = Runtime.getRuntime().availableProcessors();
    public static final ExecutorService defaultExecutor=Executors.newWorkStealingPool(coreCount);

}
