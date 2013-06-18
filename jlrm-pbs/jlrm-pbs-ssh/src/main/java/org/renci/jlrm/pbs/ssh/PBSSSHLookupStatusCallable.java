package org.renci.jlrm.pbs.ssh;

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
import org.renci.jlrm.pbs.PBSJobStatusInfo;
import org.renci.jlrm.pbs.PBSJobStatusType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

public class PBSSSHLookupStatusCallable implements Callable<Set<PBSJobStatusInfo>> {

    private final Logger logger = LoggerFactory.getLogger(PBSSSHLookupStatusCallable.class);

    private Site site;

    private List<PBSSSHJob> jobs;

    public PBSSSHLookupStatusCallable() {
        super();
    }

    public PBSSSHLookupStatusCallable(Site site, List<PBSSSHJob> jobs) {
        super();
        this.site = site;
        this.jobs = jobs;
    }

    @Override
    public Set<PBSJobStatusInfo> call() throws JLRMException {
        logger.info("ENTERING call()");

        Set<PBSJobStatusInfo> jobStatusSet = new HashSet<PBSJobStatusInfo>();

        StringBuilder sb = new StringBuilder();
        for (PBSSSHJob job : this.jobs) {
            sb.append(" ").append(job.getId());
        }
        String jobXarg = sb.toString().replaceFirst(" ", "");
        String command = String.format(". ~/.bashrc; qstat | tail -n+3 | awk '{print $1,$5,$6,$2}'", jobXarg);

        String home = System.getProperty("user.home");
        String knownHostsFilename = home + "/.ssh/known_hosts";

        JSch sch = new JSch();
        Session session = null;
        ChannelExec execChannel = null;
        try {
            sch.addIdentity(home + "/.ssh/id_rsa");
            sch.setKnownHosts(knownHostsFilename);
            session = sch.getSession(getSite().getUsername(), getSite().getSubmitHost(), 22);
            Properties config = new Properties();
            config.setProperty("StrictHostKeyChecking", "no");
            session.setConfig(config);
            session.connect(30000);

            execChannel = (ChannelExec) session.openChannel("exec");
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
                PBSJobStatusType statusType = PBSJobStatusType.ENDING;
                if (StringUtils.isNotEmpty(line)) {
                    if (line.contains("is not found")) {
                        statusType = PBSJobStatusType.ENDING;
                    } else {
                        String[] lineSplit = line.split(" ");
                        if (lineSplit != null && lineSplit.length == 2) {
                            for (PBSJobStatusType type : PBSJobStatusType.values()) {
                                if (type.getValue().equals(lineSplit[1])) {
                                    statusType = type;
                                }
                            }

                            PBSJobStatusInfo info = new PBSJobStatusInfo(lineSplit[0], statusType, lineSplit[2],
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
        } finally {
            if (execChannel != null) {
                execChannel.disconnect();
            }
            if (session != null) {
                session.disconnect();
            }
        }
        return jobStatusSet;
    }

    public Site getSite() {
        return site;
    }

    public void setSite(Site site) {
        this.site = site;
    }

    public List<PBSSSHJob> getJobs() {
        return jobs;
    }

    public void setJobs(List<PBSSSHJob> jobs) {
        this.jobs = jobs;
    }

}
