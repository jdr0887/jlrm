package org.renci.jlrm.lsf.ssh;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.LineNumberReader;
import java.io.StringReader;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.renci.jlrm.JLRMException;
import org.renci.jlrm.Site;
import org.renci.jlrm.lsf.LSFJobStatusInfo;
import org.renci.jlrm.lsf.LSFJobStatusType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

public class LSFSSHLookupStatusCallable implements Callable<Set<LSFJobStatusInfo>> {

    private final Logger logger = LoggerFactory.getLogger(LSFSSHLookupStatusCallable.class);

    private List<LSFSSHJob> jobs;

    private Site site;

    public LSFSSHLookupStatusCallable() {
        super();
    }

    public LSFSSHLookupStatusCallable(List<LSFSSHJob> jobs, Site site) {
        super();
        this.jobs = jobs;
        this.site = site;
    }

    @Override
    public Set<LSFJobStatusInfo> call() throws JLRMException {
        logger.info("ENTERING call()");

        StringBuilder sb = new StringBuilder();
        for (LSFSSHJob job : this.jobs) {
            sb.append(" ").append(job.getId());
        }
        String jobXarg = sb.toString().replaceFirst(" ", "");
        String command = String
                .format(". ~/.bashrc; bjobs %1$s | tail -n+2 | grep RUN | awk '{print $1,$3,$4,$7}' && bjobs %1$s | tail -n+2 | grep PEND | awk '{print $1,$3,$4,$6}'",
                        jobXarg);

        String home = System.getProperty("user.home");
        String knownHostsFilename = home + "/.ssh/known_hosts";

        Set<LSFJobStatusInfo> jobStatusSet = new HashSet<LSFJobStatusInfo>();
        JSch sch = new JSch();
        try {
            sch.addIdentity(home + "/.ssh/id_rsa");
            sch.setKnownHosts(knownHostsFilename);
            Session session = sch.getSession(site.getUsername(), getSite().getSubmitHost(), 22);
            Properties config = new Properties();
            config.setProperty("StrictHostKeyChecking", "no");
            session.setConfig(config);
            session.connect(30000);

            ChannelExec execChannel = (ChannelExec) session.openChannel("exec");
            execChannel.setInputStream(null);

            ByteArrayOutputStream err = new ByteArrayOutputStream();
            execChannel.setErrStream(err);

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            execChannel.setOutputStream(out);

            execChannel.setCommand(command);

            InputStream in = execChannel.getInputStream();
            execChannel.connect();

            String output = IOUtils.toString(in).trim();
            int exitCode = execChannel.getExitStatus();

            execChannel.disconnect();
            session.disconnect();

            LineNumberReader lnr = new LineNumberReader(new StringReader(output));
            String line;
            while ((line = lnr.readLine()) != null) {
                LSFJobStatusType statusType = LSFJobStatusType.DONE;
                if (StringUtils.isNotEmpty(line)) {
                    if (line.contains("is not found")) {
                        statusType = LSFJobStatusType.DONE;
                    } else {
                        String[] lineSplit = line.split(" ");
                        if (lineSplit != null && lineSplit.length == 4) {
                            for (LSFJobStatusType type : LSFJobStatusType.values()) {
                                if (type.getValue().equals(lineSplit[1])) {
                                    statusType = type;
                                }
                            }
                            LSFJobStatusInfo info = new LSFJobStatusInfo(lineSplit[0], statusType, lineSplit[2],
                                    lineSplit[3]);
                            logger.info("JobStatus is {}", info.toString());
                            jobStatusSet.add(info);
                        }
                    }
                }
            }
        } catch (JSchException e) {
            logger.error("JSchException", e);
            throw new JLRMException("JSchException: " + e.getMessage());
        } catch (IOException e) {
            logger.error("IOException", e);
            throw new JLRMException("IOException: " + e.getMessage());
        } catch (Exception e) {
            logger.error("Exception", e);
            throw new JLRMException("Exception: " + e.getMessage());
        }
        return jobStatusSet;
    }

    public Site getSite() {
        return site;
    }

    public void setSite(Site site) {
        this.site = site;
    }

    public List<LSFSSHJob> getJobs() {
        return jobs;
    }

    public void setJobs(List<LSFSSHJob> jobs) {
        this.jobs = jobs;
    }

}
