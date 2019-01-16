package lab0.dataframe.server.protocol;

public enum WorkerCommType {
    ANNOUNCE,
    HEARTBEAT,
    TASK_SCHEDULED,
    TASK_COMPLETED,
    TASK_REJECTED,
    DISCONNECT
}
