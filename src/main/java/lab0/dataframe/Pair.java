package lab0.dataframe;

import java.util.Objects;

public class Pair<F, S> {

    F first;
    S second;

    Pair(F f, S s) {
        first = f;
        second = s;
    }

    public F getFirst() {
        return first;
    }

    public void setFirst(F first) {
        this.first = first;
    }

    public S getSecond() {
        return second;
    }

    public void setSecond(S second) {
        this.second = second;
    }
}
