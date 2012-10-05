package org.renci.jlrm.condor;

import org.jgrapht.ext.VertexNameProvider;

public class CondorJobEdgeNameProvider implements VertexNameProvider<CondorJob> {

    @Override
    public String getVertexName(CondorJob job) {
        return job.getName();
    }

}
