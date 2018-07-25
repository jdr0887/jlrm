package org.renci.jlrm.condor;

public enum CondorDAGJobStatusType {

    OK(0),

    ERROR(1),

    FAILED_NODES(2),

    ABORTED(3),

    REMOVED(4),

    CYCLE_FOUND(5),

    SUSPENDED(6);

    private Integer code;

    private CondorDAGJobStatusType(Integer code) {
        this.code = code;
    }

    public Integer getCode() {
        return code;
    }

    public void setCode(Integer code) {
        this.code = code;
    }

}
