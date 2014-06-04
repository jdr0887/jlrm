package org.renci.jlrm.condor;

import org.jgrapht.graph.DefaultEdge;

public class CondorJobEdge extends DefaultEdge {

    private static final long serialVersionUID = 3289410868411756519L;

    private String inputLabel = "";

    public CondorJobEdge() {
        super();
    }

    @Override
    public Object getSource() {
        return super.getSource();
    }

    @Override
    public Object getTarget() {
        return super.getTarget();
    }

    public String getInputLabel() {
        return inputLabel;
    }

    public void setInputLabel(String inputLabel) {
        this.inputLabel = inputLabel;
    }

}
