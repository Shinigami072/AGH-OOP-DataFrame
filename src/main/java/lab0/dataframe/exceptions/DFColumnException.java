package lab0.dataframe.exceptions;

public class DFColumnException extends DFException {

    String colname;
    int num;

    public DFColumnException(String message, String colname, int id) {
        super(message);
        this.colname = colname;
        this.num = id;
    }
}
