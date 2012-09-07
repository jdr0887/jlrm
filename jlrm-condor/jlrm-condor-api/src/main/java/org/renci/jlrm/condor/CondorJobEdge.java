package org.renci.jlrm.condor;

import java.util.List;

import org.jgrapht.graph.DefaultEdge;

public class CondorJobEdge extends DefaultEdge {

    private static final long serialVersionUID = 3289410868411756519L;

    private List<String> inputLabelList;

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

    public List<String> getInputLabelList() {
        return inputLabelList;
    }

    public void setInputLabelList(List<String> inputLabelList) {
        this.inputLabelList = inputLabelList;
    }

}
