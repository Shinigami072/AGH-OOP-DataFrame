package lab0.dataframe.values;

import java.util.concurrent.LinkedBlockingQueue;

public class BlockingCloseableQueue<E> extends LinkedBlockingQueue<E> {

    boolean closed = false;

    boolean isClosed() {
        return closed;
    }

    void close() {
        closed = true;
    }


    @Override
    public E take() throws InterruptedException {
        return super.take();
    }
}
