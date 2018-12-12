package lab0.dataframe.exceptions;

public class DFZeroLengthCreationException extends DFUncheckedException {
    public DFZeroLengthCreationException() {
        super("0 length DF creation");
    }
}
