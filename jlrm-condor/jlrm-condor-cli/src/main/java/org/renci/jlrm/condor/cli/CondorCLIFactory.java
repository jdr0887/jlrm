package org.renci.jlrm.condor.cli;

import java.io.File;
import java.util.List;

import org.jgrapht.Graph;
import org.renci.jlrm.LRMException;
import org.renci.jlrm.condor.ClassAdvertisement;
import org.renci.jlrm.condor.CondorJob;
import org.renci.jlrm.condor.CondorJobEdge;
import org.renci.jlrm.condor.CondorJobStatusType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author jdr0887
 */
public class CondorCLIFactory {

    private final Logger logger = LoggerFactory.getLogger(CondorCLIFactory.class);

    private static CondorCLIFactory instance = null;

    private File condorHome;

    public static CondorCLIFactory getInstance(File condorHome) {
        if (instance == null) {
            instance = new CondorCLIFactory(condorHome);
        }
        return instance;
    }

    private CondorCLIFactory(File condorHome) {
        super();
        this.condorHome = condorHome;
    }

    public CondorJob submit(File submitDir, CondorJob job) throws LRMException {
        logger.debug("ENTERING submit(File, CondorJob)");
        CondorSubmitCallable runnable = new CondorSubmitCallable(this.condorHome, submitDir, job);
        return runnable.call();
    }

    public CondorJob submit(String dagName, File submitDir, Graph<CondorJob, CondorJobEdge> graph) throws LRMException {
        logger.debug("ENTERING submit(String dagName, File submitDir, Graph<CondorJob, CondorJobEdge> graph)");
        CondorSubmitDAGCallable runnable = new CondorSubmitDAGCallable(this.condorHome, submitDir, graph, dagName);
        return runnable.call();
    }

    public CondorJobStatusType lookupStatus(CondorJob jobNode) throws LRMException {
        logger.debug("ENTERING lookupStatus(JobNode)");
        CondorLookupStatusCallable runnable = new CondorLookupStatusCallable(this.condorHome, jobNode);
        return runnable.call();
    }

    public List<CondorJob> listGlideinJobs(List<ClassAdvertisement> classAdList) throws LRMException {
        logger.debug("ENTERING findJobByClassAdvertisement(List<ClassAdvertisement> classAdList)");
        return null;
    }

}
