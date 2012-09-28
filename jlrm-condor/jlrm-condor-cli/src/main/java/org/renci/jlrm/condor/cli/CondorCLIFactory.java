package org.renci.jlrm.condor.cli;

import java.io.File;
import java.util.List;
import java.util.Map;

import org.jgrapht.Graph;
import org.renci.jlrm.JLRMException;
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

    public static CondorCLIFactory getInstance() {
        if (instance == null) {
            instance = new CondorCLIFactory();
        }
        return instance;
    }

    private CondorCLIFactory() {
        super();
    }

    public Map<String, List<ClassAdvertisement>> lookupJobsByOwner(String owner) throws JLRMException {
        logger.debug("ENTERING lookupJobsByUsername(String username)");
        CondorLookupJobsByOwnerCallable runnable = new CondorLookupJobsByOwnerCallable(owner);
        return runnable.call();
    }
    
    public CondorJob submit(File submitDir, CondorJob job) throws JLRMException {
        logger.debug("ENTERING submit(File, CondorJob)");
        CondorSubmitCallable runnable = new CondorSubmitCallable(submitDir, job);
        return runnable.call();
    }

    public CondorJob submit(String dagName, File submitDir, Graph<CondorJob, CondorJobEdge> graph) throws JLRMException {
        logger.debug("ENTERING submit(String dagName, File submitDir, Graph<CondorJob, CondorJobEdge> graph)");
        CondorSubmitDAGCallable runnable = new CondorSubmitDAGCallable(submitDir, graph, dagName);
        return runnable.call();
    }

    public CondorJobStatusType lookupStatus(CondorJob jobNode) throws JLRMException {
        logger.debug("ENTERING lookupStatus(JobNode)");
        CondorLookupStatusCallable runnable = new CondorLookupStatusCallable(jobNode);
        return runnable.call();
    }

    public List<CondorJob> listGlideinJobs(List<ClassAdvertisement> classAdList) throws JLRMException {
        logger.debug("ENTERING findJobByClassAdvertisement(List<ClassAdvertisement> classAdList)");
        return null;
    }

}
