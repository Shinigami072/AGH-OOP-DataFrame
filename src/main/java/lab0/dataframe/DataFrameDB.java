package lab0.dataframe;

import lab0.dataframe.exceptions.*;
import lab0.dataframe.groupby.Applyable;
import lab0.dataframe.groupby.GroupBy;
import lab0.dataframe.values.*;

import java.sql.*;
import java.util.*;

public class DataFrameDB extends DataFrame implements AutoCloseable {


    private final String tableName;
    private final String[] colNames;
    private final Map<String, Integer> indexMap;

    private final Class<? extends Value>[] types;
    private final Value.ValueBuilder[] factories;
    private DBConnection connection;
    private Statement stmt;
    private ResultSet rs;

    protected DataFrameDB(DBConnection connection, String tableName, String[] colNames, Class<? extends Value>[] types) {
        super(colNames.length);
        this.connection = connection;
        this.tableName = tableName;
        this.colNames = colNames;
        this.types = types;
        this.rowNumber = -1;
        this.indexMap = new HashMap<>();

        this.factories = new Value.ValueBuilder[colNames.length];
        for (int i = 0; i < colNames.length; i++) {
            factories[i] = Value.builder(types[i]);
            indexMap.put(colNames[i], i);
        }
        size();

    }

    public static Builder getBuilder() {
        return new Builder();
    }

    public static DataFrame fromSelect(ResultSet select) throws SQLException, DFValueBuildException {

        ResultSetMetaData mt = select.getMetaData();
        int colCount = mt.getColumnCount();
        String[] names = new String[colCount];
        Class<? extends Value>[] types = new Class[colCount];
        Value.ValueBuilder[] factories = new Value.ValueBuilder[colCount];

        for (int i = 0; i < colCount; i++) {
            names[i] = mt.getColumnName(i + 1);
            types[i] = TypeEnum.Type.getValueType(mt.getColumnType(i + 1));
            factories[i] = Value.builder(types[i]);
        }


        DataFrame df = new DataFrame(names, types);
        Value[] row = new Value[colCount];

        try {

            while (select.next()) {
                for (int i = 0; i < colCount; i++) {
                    row[i] = factories[i].build(select.getString(i + 1));
                }

                df.addRecord(row);

            }
        } catch (DFColumnTypeException ignore) {
        }
        select.close();

        return df;
    }

    private Statement getStatement() throws SQLException {
        if (stmt == null || stmt.isClosed()) {
            stmt = connection.connect().createStatement();
        }
        return stmt;
    }

    public DataFrame toDataFrame() throws SQLException, DFValueBuildException {
        getStatement();
        rs = stmt.executeQuery(String.format("select * from %s", tableName));
        return fromSelect(rs);
    }

    @Override
    public String[] getNames() {
        return colNames;
    }

    @Override
    public Class<? extends Value>[] getTypes() {
        return types;
    }

    @Override
    public void addRecord(Value... values) throws DFDimensionException {
        if (values.length != getColCount())
            throw new DFDimensionException("");//todo: better

        try {
            getStatement();

            StringBuilder s = new StringBuilder();
            for (int i = 0; i < values.length; i++) {
                Value v = values[i];
                s.append("'").append(v.toString()).append("'");

                if (i < values.length - 1)
                    s.append(",");

            }

            String valuesString = s.toString();

            rowNumber += stmt.executeUpdate(String.format("INSERT into %s VALUES (%s)", tableName, valuesString));

        } catch (SQLException e) {
            e.printStackTrace();
            throw new DFDimensionException("");//todo: better

        }
    }

    @Override
    public DataFrame iloc(int from, int to) throws DFColumnTypeException {
        try {
            getStatement();

            rs = stmt.executeQuery(String.format("SELECT * FROM %s limit %d offset %d", tableName, (to + 1 - from), from));
            return fromSelect(rs);

        } catch (SQLException | DFValueBuildException e) {
            e.printStackTrace();
        }
        throw new RuntimeException(":REGEASRg"); //todo: better
    }

    @Override
    public DataFrame get(String[] cols, boolean copy) throws DFZeroLengthCreationException, CloneNotSupportedException {
        try {
            getStatement();

            rs = stmt.executeQuery(String.format("SELECT %s FROM %s ", String.join(",", cols), tableName));
            return fromSelect(rs);

        } catch (SQLException | DFValueBuildException e) {
            e.printStackTrace();
        }
        throw new RuntimeException(":REGEASRg"); //todo: better

    }

//    @Override
//    public int getColCount() {
//        return colNames.length;
//    }

    @Override
    public int size() {
        try {
            getStatement();

            rs = stmt.executeQuery(String.format("SELECT count(*) FROM %s", tableName));
            rs.next();

            rowNumber = rs.getInt(1);
            rs.close();
            rs = null;

            return rowNumber;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return rowNumber;
    }

    @Override
    public Value[] getRecord(int i) {
        Value[] row = new Value[getColCount()];

        try {
            getStatement();
            rs = stmt.executeQuery(String.format("SELECT * FROM %s limit 1 offset %d", tableName, i));
            rs.next();

            for (int j = 0; j < getColCount(); j++) {
                Value.ValueBuilder vb = factories[j];
                String s = rs.getString(colNames[j]);
                Value v = vb.build(s);
                row[j] = v;
            }


        } catch (SQLException e) {
            e.printStackTrace();
        } catch (DFValueBuildException e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException ignore) {
                }
                rs = null;
            }
        }

        return row;
    }

    /**/
    @Override
    public DBColumn get(String colname) {

        int index = indexMap.get(colname);

        DBColumn kol = (DBColumn) kolumny[index];

        if (kol == null) {
            kol = new DBColumn(index);
            kolumny[index] = kol;
        }

        return kol;
    }

    @Override
    public void close() throws SQLException {
        connection.close();
        if (stmt != null) {
            stmt.close();
        }
        if (rs != null) {
            rs.close();
        }

        for (Column k : kolumny) {
            if (k instanceof DBColumn) {
                ((DBColumn) k).close();
            }

        }
    }

    @Override
    public String toString() {
        StringBuilder s = new StringBuilder();

        for (int i = 0; i < getColCount(); i++) {
            s.append(String.format("|%-14.14s:%15.15s", colNames[i], types[i].getSimpleName()));
        }

        s.append("|\n");

        try {
            getStatement();
            rs = stmt.executeQuery(String.format("SELECT * FROM %s", tableName));

            while (rs.next()) {
                for (int j = 0; j < getColCount(); j++)
                    s.append(String.format("|%30s", rs.getString(colNames[j])));

                s.append("|\n");

            }

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            rs = null;
        }

        return s.toString();
    }

    @Override
    public GroupBy groupBy(String... colname) throws CloneNotSupportedException {
        return new GroupByDF(colname);
    }

    protected static class DBConnection implements AutoCloseable {
        Connection connection;
        String url;
        String user;
        String password;

        DBConnection(String url, String user, String password) {
            this.url = url;
            this.user = user;
            this.password = password;
        }

        public Connection connect() throws SQLException {
            if (connection == null || connection.isClosed())
                connection = DriverManager.getConnection(url, user, password);
            return connection;
        }

        @Override
        public void close() throws SQLException {
            if (connection != null && !connection.isClosed())
                connection.close();
        }
    }

    public static class Builder {

        String url;
        String user;
        String password;
        String tableName;

        public Builder setUrl(String url) {
            this.url = url;
            return this;
        }

        public Builder setLogin(String user, String password) {
            this.user = user;
            this.password = password;
            return this;
        }

        public Builder setName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public DataFrameDB build() throws SQLException {
            if (url != null && password != null && user != null) {

                DBConnection connection = new DBConnection(url, user, password);
                Connection conn = connection.connect();
                ResultSet rs = conn.getMetaData().getColumns(null, null, tableName, null);

                ArrayList<Class<? extends Value>> typeList = new ArrayList<>();
                ArrayList<String> nameList = new ArrayList<>();

                while (rs.next()) {
                    nameList.add(rs.getString("COLUMN_NAME"));
                    typeList.add(TypeEnum.Type.getValueType(rs.getInt("DATA_TYPE")));
//                    System.out.println(rs.getString("TYPE_NAME"));
//                    System.out.println(rs.getString("NULLABLE"));
//                    System.out.println(rs.getString("COLUMN_DEF"));
//                    System.out.println(rs.getString("IS_NULLABLE"));
//                    System.out.println(rs.getObject("COLUMN_SIZE"));

                }

                Class<? extends Value>[] types = typeList.toArray(new Class[0]);
                String[] colNames = nameList.toArray(new String[0]);
                rs.close();
                DataFrameDB db = new DataFrameDB(connection, tableName, colNames, types);

                return db;

            }
            throw new IllegalArgumentException("something is no yes");//todo: proper handling
        }

    }

    protected class DBColumn extends Column implements AutoCloseable {

        private int index;

        public DBColumn(int i) {
            super(colNames[i], types[i]);
            index = i;
        }

        public Column toColumn() throws SQLException, DFValueBuildException {
            Column newCol = new Column(getName(), getType());
            getStatement();
            rs = stmt.executeQuery(String.format("SELECT %s FROM %s", getName(), tableName));
            try {

                while (rs.next()) {
                    newCol.add(factories[index].build(rs.getString(getName())));
                }
                rs.close();
                rs = null;
            } catch (DFColumnTypeException ignore) {
                ignore.printStackTrace();
            }

            return newCol;
        }

        @Override
        public int size() {
            try {
                getStatement();

                rs = stmt.executeQuery(String.format("SELECT count(%s) FROM %s", getName(), tableName));
                rs.next();

                return rs.getInt(1);
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return -1;
        }


        @Override
        public int uniqueSize() {
            try {
                getStatement();

                rs = stmt.executeQuery(String.format("SELECT distinct count(%s) FROM %s", getName(), tableName));
                rs.next();

                return rs.getInt(1);
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return -1;
        }//todo: zamykanie kolumny  i exceptopn proprtly

        @Override
        public Value get(int index) {
            Value v = null;
            try {
                getStatement();
                rs = stmt.executeQuery(String.format("SELECT %s FROM %s limit 1 offset %d", getName(), tableName, index));
                rs.next();

                Value.ValueBuilder vb = factories[this.index];
                String s = rs.getString(getName());
                v = vb.build(s);

            } catch (SQLException e) {
                e.printStackTrace();
            } catch (DFValueBuildException e) {
                e.printStackTrace();
            }

            return v;
        }

        @Override
        protected Column performOperation(Value.OPERATION_TYPES operation, Value operand) throws DFColumnTypeException {
            try {
                return toColumn().performOperation(operation, operand);
            } catch (SQLException | DFValueBuildException e) {
                e.printStackTrace();
                throw new DFColumnTypeException(this, operand, -1);
            }
        }

        @Override
        protected Column performOperation(Value.OPERATION_TYPES operation, Column operand) throws DFColumnTypeException {
            try {
                return toColumn().performOperation(operation, operand);
            } catch (SQLException | DFValueBuildException e) {
                e.printStackTrace();
                throw new DFColumnTypeException(this, operand.get(0), -1);
            }
        }

        @Override
        protected DBColumn clone() throws CloneNotSupportedException {
            throw new CloneNotSupportedException();
        }

        @Override
        public int hashCode() {
            return super.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            return super.equals(obj);
        }

        @Override
        public void add(Value o) throws DFColumnTypeException {
            try {
                getStatement();

                String value = o.toString();

                rowNumber += stmt.executeUpdate(String.format("INSERT into %s(%s) VALUES (%s)", tableName, getName(), value));

            } catch (SQLException e) {
                e.printStackTrace();
                throw new DFDimensionException("");//todo: better

            }
        }

        @Override
        public Column copy() throws CloneNotSupportedException {
            try {
                return toColumn();
            } catch (SQLException | DFValueBuildException e) {
                e.printStackTrace();
                throw new CloneNotSupportedException();
            }
        }

        @Override
        public String toString() {
            StringBuilder s = new StringBuilder();

            s.append(String.format("|%-14.14s:%15.15s", getName(), getType().getSimpleName()));


            s.append("|\n");

            try {
                getStatement();
                rs = stmt.executeQuery(String.format("SELECT %s FROM %s", getName(), tableName));

                while (rs.next()) {
                    s.append(String.format("|%30.30s", rs.getString(getName())));
                    s.append("|\n");
                }

            } catch (SQLException e) {
                e.printStackTrace();
            }

            return s.toString();
        }

        public void close() throws SQLException {
            if (connection != null)
                connection.close();
        }
    }

    class GroupByDF implements GroupBy {

        String groupby;
        String[] other;
        String[] otherNumeric;
        Map<Integer, DataFrame> groupedDF;
        DataFrame values;

        GroupByDF(String... colname) {

            groupby = String.join(", ", colname);
            ArrayList<String> ls = new ArrayList<String>(Arrays.asList(getNames()));
            ls.removeAll(Arrays.asList(colname));

            other = ls.toArray(new String[0]);

            Class<? extends Value>[] types = getTypes();

            ls.removeIf(_colname ->
                    !NumericValue.class.
                            isAssignableFrom(types[indexMap.get(_colname)])
            );

            otherNumeric = ls.toArray(new String[0]);

        }

        @Override
        public DataFrame max() throws DFApplyableException {
            if (other.length == 0)
                throw new DFApplyableException("stuff");
            return getApplied(otherFunction("max", other));

        }

        @Override
        public DataFrame min() throws DFApplyableException {
            if (other.length == 0)
                throw new DFApplyableException("stuff");
            return getApplied(otherFunction("min", other));
        }

        @Override
        public DataFrame mean() throws DFApplyableException {
            if (otherNumeric.length == 0)
                throw new DFApplyableException("stuff");
            return getApplied(otherFunction("avg", otherNumeric));
        }

        @Override
        public DataFrame sum() throws DFApplyableException {
            if (otherNumeric.length == 0)
                throw new DFApplyableException("stuff");
            return getApplied(otherFunction("sum", otherNumeric));
        }

        @Override
        public DataFrame var() throws DFApplyableException {
            if (otherNumeric.length == 0)
                throw new DFApplyableException("stuff");
            return getApplied(otherFunction("variance", otherNumeric));
        }

        @Override
        public DataFrame std() throws DFApplyableException {
            if (otherNumeric.length == 0)
                throw new DFApplyableException("stuff");
            return getApplied(otherFunction("std", otherNumeric));
        }

        private String otherFunction(String func, String[] other) {
            StringBuilder s = new StringBuilder();
            for (int i = 0; i < other.length; i++) {
                s.append(func).append("(").append(other[i]).append(") as ").append(other[i]);
                if (i < other.length - 1)
                    s.append(", ");
            }
            return s.toString();
        }

        private DataFrame getApplied(String operation) throws DFApplyableException {
            try {
                getStatement();
                String sql;
                if (groupby.length() > 0)
//                    //"select "+groupby+", "+operation+" from "+tableName+" group by "+groupby+" order by "+groupby; //
                    sql = String.format("select %1$s,%2$s from %3$s group by %1$s order by %1$s", groupby, operation, tableName);
                else
                    sql = String.format("select %s from %s order by %s", operation, tableName, operation);

//                System.out.println(sql);
                rs = stmt.executeQuery(sql);
                return fromSelect(rs);
            } catch (Exception e) {
                throw new DFApplyableException(e.getMessage());
            }
        }

        @Override
        public DataFrame apply(Applyable apply) throws DFApplyableException {
            if (groupedDF == null) {
                initGroups();
            }
            try {

                DataFrame output = null;

                for (int i = 0; i < values.size(); i++) {

                    DataFrame group = apply.apply(groupedDF.get(i));

                    if (output == null) {
                        output = getOutputDataFrame(group);
                    }

                    Value[] keyValues = values.getRecord(i);
                    Value[] rowValues = new Value[group.getColCount() + values.getColCount()];

                    System.arraycopy(keyValues, 0, rowValues, 0, keyValues.length);

                    for (int j = 0; j < group.size(); j++) {
                        Value[] groupValues = group.getRecord(j);
                        System.arraycopy(groupValues, 0, rowValues, keyValues.length, groupValues.length);
                        output.addRecord(rowValues);

                    }


                }

                return output;

            } catch (DFColumnTypeException e) {
                throw new DFApplyableException(e.getMessage());
            }

        }

        private DataFrame getOutputDataFrame(DataFrame group) {
            DataFrame output;
            Class<? extends Value>[] keyTypes = values.getTypes();
            Class<? extends Value>[] dfTypes = group.getTypes();
            Class<? extends Value>[] fullTypes = new Class[keyTypes.length + dfTypes.length];

            System.arraycopy(keyTypes, 0, fullTypes, 0, keyTypes.length);
            System.arraycopy(dfTypes, 0, fullTypes, keyTypes.length, dfTypes.length);

            String[] keyNames = values.getNames();
            String[] dfNames = group.getNames();
            String[] fullNames = new String[keyNames.length + dfNames.length];

            System.arraycopy(keyNames, 0, fullNames, 0, keyNames.length);
            System.arraycopy(dfNames, 0, fullNames, keyNames.length, dfNames.length);

            output = new DataFrame(fullNames, fullTypes);
            return output;
        }

        private void initGroups() throws DFApplyableException {
            try {
                getStatement();

                rs = stmt.executeQuery(String.format("select distinct %s from %s order by %s", groupby, tableName, groupby));
                values = fromSelect(rs);

                String otherNames = String.join(",", other);

                groupedDF = new HashMap<>();
                String[] group_col_id = groupby.split(", ");
                for (int i = 0; i < values.size(); i++) {
                    Value[] row = values.getRecord(i);
                    String key = getCondition(group_col_id, row);

                    rs = stmt.executeQuery(String.format("select %s from %s where %s", otherNames, tableName, key));
                    DataFrame group = fromSelect(rs);

                    groupedDF.put(i, group);
                }
                rs.close();

            } catch (SQLException | DFValueBuildException e) {
                groupedDF = null;
                throw new DFApplyableException(e.getMessage());
            } finally {
                rs = null;
            }
        }

        private String getCondition(String[] group_col_id, Value[] row) {
            StringBuilder s = new StringBuilder();
            for (int j = 0; j < values.getColCount(); j++) {
                s.append(group_col_id[j]).append('=');

                if (!(row[j] instanceof StringValue))
                    s.append(row[j].toString());
                else {
                    s.append("'");
                    s.append(row[j].toString());
                    s.append("'");
                }

                if (j < values.getColCount() - 1)
                    s.append(" and ");
            }
            return s.toString();
        }
    }
}
