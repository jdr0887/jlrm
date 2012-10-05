package org.renci.jlrm.condor;

import org.jgrapht.ext.VertexNameProvider;

public class CondorJobVertexNameProvider implements VertexNameProvider<CondorJob> {

    @Override
    public String getVertexName(CondorJob job) {
        return job.getName();
    }

}
