package org.renci.jlrm.lsf.ssh;

import java.io.File;

import org.renci.jlrm.LRMException;
import org.renci.jlrm.lsf.LSFJobStatusType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author jdr0887
 */
public class LSFSSHFactory {

    private final Logger logger = LoggerFactory.getLogger(LSFSSHFactory.class);

    private static LSFSSHFactory instance = null;

    private File lsfHome;

    private String username;

    private String submitHost;

    public static LSFSSHFactory getInstance(File lsfHome, String submitHost) {
        if (instance == null) {
            instance = new LSFSSHFactory(lsfHome, System.getProperty("user.name"), submitHost);
        }
        return instance;
    }

    public static LSFSSHFactory getInstance(File lsfHome, String username, String submitHost) {
        if (instance == null) {
            instance = new LSFSSHFactory(lsfHome, username, submitHost);
        }
        return instance;
    }

    private LSFSSHFactory(File lsfHome, String username, String submitHost) {
        super();
        this.lsfHome = lsfHome;
        this.submitHost = submitHost;
        this.username = username;
    }

    public LSFSSHJob submit(File submitDir, LSFSSHJob job) throws LRMException {
        logger.debug("ENTERING submit(File)");
        LSFSSHSubmitCallable runnable = new LSFSSHSubmitCallable(this.lsfHome, this.username, this.submitHost, job,
                submitDir);
        return runnable.call();
    }

    public LSFSSHJob submitGlidein(File submitDir, Integer maxNoClaimTime, Integer maxRunTime, Integer requireMemory, String collectorHost, String queue)
            throws LRMException {
        logger.debug("ENTERING submit(File)");
        LSFSSHSubmitCondorGlideinCallable runnable = new LSFSSHSubmitCondorGlideinCallable();
        runnable.setLsfHome(this.lsfHome);
        runnable.setMaxNoClaimTime(maxNoClaimTime);
        runnable.setMaxRunTime(maxRunTime);
        runnable.setRequiredMemory(requireMemory);
        runnable.setSubmitHost(this.submitHost);
        runnable.setSubmitDir(submitDir);
        runnable.setCollectorHost(collectorHost);
        runnable.setUsername(this.username);
        runnable.setQueue(queue);
        return runnable.call();
    }

    public LSFJobStatusType lookupStatus(LSFSSHJob job) throws LRMException {
        logger.debug("ENTERING lookupStatus(job)");
        LSFSSHLookupStatusCallable runnable = new LSFSSHLookupStatusCallable(this.lsfHome, this.username,
                this.submitHost, job);
        return runnable.call();
    }

}
