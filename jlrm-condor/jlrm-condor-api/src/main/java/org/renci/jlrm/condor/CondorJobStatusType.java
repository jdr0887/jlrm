package org.renci.jlrm.condor;

public enum CondorJobStatusType {

    UNEXPANDED(0),

    IDLE(1),

    RUNNING(2),

    REMOVED(3),

    COMPLETED(4),

    HELD(5),

    SUBMISSION_ERROR(6);

    private int code;

    private CondorDAGJobStatusType dagJobStatusType;

    private CondorJobStatusType(int code) {
        this.code = code;
    }

    private CondorJobStatusType(int code, CondorDAGJobStatusType dagJobStatusType) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public CondorDAGJobStatusType getDagJobStatusType() {
        return dagJobStatusType;
    }

    public void setDagJobStatusType(CondorDAGJobStatusType dagJobStatusType) {
        this.dagJobStatusType = dagJobStatusType;
    }

}
