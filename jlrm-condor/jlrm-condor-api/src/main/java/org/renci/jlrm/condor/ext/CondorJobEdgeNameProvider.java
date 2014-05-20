package org.renci.jlrm.condor.ext;

import org.jgrapht.ext.VertexNameProvider;
import org.renci.jlrm.condor.CondorJob;

public class CondorJobEdgeNameProvider implements VertexNameProvider<CondorJob> {

    @Override
    public String getVertexName(CondorJob job) {
        return job.getName();
    }

}
