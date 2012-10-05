package org.renci.jlrm.lsf.cli;

import java.io.File;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang.StringUtils;
import org.renci.jlrm.JLRMException;
import org.renci.jlrm.lsf.LSFJob;
import org.renci.jlrm.lsf.LSFJobStatusType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author jdr0887
 */
public class LSFCLIFactory {

    private final Logger logger = LoggerFactory.getLogger(LSFCLIFactory.class);

    private static LSFCLIFactory instance = null;

    private File lsfHomeDirectory;

    public static LSFCLIFactory getInstance() throws JLRMException {
        if (instance == null) {
            instance = new LSFCLIFactory();
        }
        return instance;
    }

    private LSFCLIFactory() throws JLRMException {
        super();
        String lsfHome = System.getenv("LSF_HOME");
        if (StringUtils.isEmpty(lsfHome)) {
            logger.error("LSF_HOME not set in env: {}", lsfHome);
            throw new JLRMException("LSF_HOME not set in env");
        }
        this.lsfHomeDirectory = new File(lsfHome);
        if (!lsfHomeDirectory.exists()) {
            logger.error("LSF_HOME does not exist: {}", lsfHomeDirectory);
            throw new JLRMException("LSF_HOME does not exist");
        }
    }

    public LSFJob submit(File submitDir, LSFJob job) throws JLRMException {
        logger.debug("ENTERING submit(File)");
        ExecutorService executor = Executors.newSingleThreadExecutor();
        LSFJob ret = null;
        try {
            ret = executor.submit(new LSFSubmitCallable(this.lsfHomeDirectory, job, submitDir)).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return ret;
    }

    public LSFJobStatusType lookupStatus(LSFJob job) throws JLRMException {
        logger.debug("ENTERING lookupStatus(job)");
        ExecutorService executor = Executors.newSingleThreadExecutor();
        LSFJobStatusType ret = null;
        try {
            ret = executor.submit(new LSFLookupStatusCallable(this.lsfHomeDirectory, job)).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return ret;
    }

}
