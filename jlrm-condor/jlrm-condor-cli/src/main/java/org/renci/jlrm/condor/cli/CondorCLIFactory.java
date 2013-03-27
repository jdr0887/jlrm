package org.renci.jlrm.condor.cli;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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

    public static CondorCLIFactory getInstance() throws JLRMException {
        if (instance == null) {
            instance = new CondorCLIFactory();
        }
        return instance;
    }

    private CondorCLIFactory() throws JLRMException {
        super();
    }

    public Map<String, List<ClassAdvertisement>> lookupJobsByOwner(String owner) throws JLRMException {
        logger.info("ENTERING lookupJobsByUsername(String username)");
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Map<String, List<ClassAdvertisement>> ret = null;
        try {
            ret = executor.submit(new CondorLookupJobsByOwnerCallable(owner)).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return ret;
    }

    public CondorJob submit(File submitDir, CondorJob job) throws JLRMException {
        logger.info("ENTERING submit(File, CondorJob)");
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CondorJob ret = null;
        try {
            ret = executor.submit(new CondorSubmitCallable(submitDir, job)).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return ret;
    }

    public CondorJob submit(String dagName, File submitDir, Graph<CondorJob, CondorJobEdge> graph) throws JLRMException {
        logger.info("ENTERING submit(String dagName, File submitDir, Graph<CondorJob, CondorJobEdge> graph)");
        return submit(dagName, submitDir, graph, true);
    }

    public CondorJob submit(String dagName, File submitDir, Graph<CondorJob, CondorJobEdge> graph,
            Boolean includeGlideinRequirements) throws JLRMException {
        logger.info("ENTERING submit(String dagName, File submitDir, Graph<CondorJob, CondorJobEdge> graph, Boolean includeGlideinRequirements)");
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CondorJob ret = null;
        try {
            ret = executor.submit(new CondorSubmitDAGCallable(submitDir, graph, dagName, includeGlideinRequirements))
                    .get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return ret;
    }

    public CondorJobStatusType lookupStatus(CondorJob jobNode) throws JLRMException {
        logger.info("ENTERING lookupStatus(JobNode)");
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CondorJobStatusType ret = null;
        try {
            ret = executor.submit(new CondorLookupStatusCallable(jobNode)).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return ret;
    }

    public Map<String, CondorJobStatusType> lookupDAGStatus(CondorJob job) throws JLRMException {
        logger.info("ENTERING lookupDAGStatus(CondorJob)");
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Map<String, CondorJobStatusType> ret = null;
        try {
            ret = executor.submit(new CondorLookupDAGStatusCallable(job)).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return ret;
    }

}
