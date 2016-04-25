package org.renci.jlrm.condor;

public enum CondorDAGJobStatusType {

    OK(0),

    ERROR(1),

    FAILED_NODES(2),

    ABORTED(3),

    REMOVED(4),

    CYCLE_FOUND(5),

    SUSPENDED(6);

    private int code;

    private CondorDAGJobStatusType(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

}
