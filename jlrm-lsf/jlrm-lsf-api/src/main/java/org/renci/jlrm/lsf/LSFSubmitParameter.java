package org.renci.jlrm.lsf;

public class LSFSubmitParameter {

    private String flag;

    private String value;

    public LSFSubmitParameter() {
        super();
    }

    public LSFSubmitParameter(String flag, String value) {
        super();
        this.flag = flag;
        this.value = value;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

}
