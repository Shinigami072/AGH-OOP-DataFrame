package lab0.dataframe;

public class Pair<F, S> {

    private F first;
    private S second;

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
