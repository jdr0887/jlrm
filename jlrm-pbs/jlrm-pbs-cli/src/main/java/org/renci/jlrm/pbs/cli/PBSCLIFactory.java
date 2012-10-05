package org.renci.jlrm.pbs.cli;

import java.io.File;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang.StringUtils;
import org.renci.jlrm.JLRMException;
import org.renci.jlrm.pbs.PBSJob;
import org.renci.jlrm.pbs.PBSJobStatusType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author jdr0887
 */
public class PBSCLIFactory {

    private final Logger logger = LoggerFactory.getLogger(PBSCLIFactory.class);

    private static PBSCLIFactory instance = null;

    private File pbsHomeDirectory;

    public static PBSCLIFactory getInstance() throws JLRMException {
        if (instance == null) {
            instance = new PBSCLIFactory();
        }
        return instance;
    }

    private PBSCLIFactory() throws JLRMException {
        super();

        String pbsHome = System.getenv("PBS_HOME");
        if (StringUtils.isEmpty(pbsHome)) {
            logger.error("PBS_HOME not set in env: {}", pbsHome);
            throw new JLRMException("PBS_HOME not set in env");
        }
        this.pbsHomeDirectory = new File(pbsHome);
        if (!pbsHomeDirectory.exists()) {
            logger.error("PBS_HOME does not exist: {}", pbsHomeDirectory);
            throw new JLRMException("PBS_HOME does not exist");
        }

    }

    public PBSJob submit(File submitDir, PBSJob job) throws JLRMException {
        logger.debug("ENTERING submit(File)");
        ExecutorService executor = Executors.newSingleThreadExecutor();
        PBSJob ret = null;
        try {
            ret = executor.submit(new PBSSubmitCallable(this.pbsHomeDirectory, job, submitDir)).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return ret;
    }

    public PBSJobStatusType lookupStatus(PBSJob job) throws JLRMException {
        logger.debug("ENTERING lookupStatus(job)");
        ExecutorService executor = Executors.newSingleThreadExecutor();
        PBSJobStatusType ret = null;
        try {
            ret = executor.submit(new PBSLookupStatusCallable(this.pbsHomeDirectory, job)).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return ret;
    }

}
