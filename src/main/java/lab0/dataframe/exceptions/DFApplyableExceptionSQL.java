package lab0.dataframe.exceptions;

import java.sql.SQLException;

public class DFApplyableExceptionSQL extends DFApplyableException {
    private SQLException exception;

    public DFApplyableExceptionSQL(SQLException e) {
        super(e.getMessage());
        exception = e;
    }

    public SQLException getException() {
        return exception;
    }
}
