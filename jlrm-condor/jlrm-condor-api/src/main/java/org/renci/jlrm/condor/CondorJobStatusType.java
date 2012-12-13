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

    private CondorJobStatusType(int code) {
        this.code = code;
    }

    /**
     * @return the code
     */
    public int getCode() {
        return code;
    }

    /**
     * @param code
     *            the code to set
     */
    public void setCode(int code) {
        this.code = code;
    }

}
