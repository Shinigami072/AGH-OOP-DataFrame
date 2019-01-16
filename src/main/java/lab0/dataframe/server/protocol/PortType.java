package lab0.dataframe.server.protocol;

public enum PortType {
    CLIENT(7700),
    WORKER(7701);

    private int port;

    PortType(int port) {
        this.port = port;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }
}
