package org.renci.jlrm.pbs.ssh;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.LineNumberReader;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.renci.jlrm.LRMException;
import org.renci.jlrm.pbs.PBSJobStatusType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

public class PBSSSHLookupStatusCallable implements Callable<Map<String, PBSJobStatusType>> {

    private final Logger logger = LoggerFactory.getLogger(PBSSSHLookupStatusCallable.class);

    private String LSFHome;

    private PBSSSHJob[] jobs;

    private String username;

    private String host;

    public PBSSSHLookupStatusCallable() {
        super();
    }

    public PBSSSHLookupStatusCallable(String LSFHome, String username, String host, PBSSSHJob... jobs) {
        super();
        this.LSFHome = LSFHome;
        this.jobs = jobs;
        this.host = host;
        this.username = username;
    }

    @Override
    public Map<String, PBSJobStatusType> call() throws LRMException {
        logger.debug("ENTERING call()");

        StringBuilder sb = new StringBuilder();
        for (PBSSSHJob job : this.jobs) {
            sb.append(" ").append(job.getId());
        }
        String jobXarg = sb.toString().replaceFirst(" ", "");
        String command = String.format("%s/bin/qstat %s | tail -n+2 | awk '{print $1,$3}'", this.LSFHome, jobXarg);

        String home = System.getProperty("user.home");
        String knownHostsFilename = home + "/.ssh/known_hosts";

        Map<String, PBSJobStatusType> jobStatusMap = new HashMap<String, PBSJobStatusType>();
        JSch sch = new JSch();
        try {
            sch.addIdentity(home + "/.ssh/id_rsa");
            sch.setKnownHosts(knownHostsFilename);
            Session session = sch.getSession(this.username, this.host, 22);
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
                            logger.info("JobStatus for {} is {}", lineSplit[0], statusType);
                            jobStatusMap.put(lineSplit[0], statusType);
                        }
                    }
                }
            }
        } catch (JSchException e) {
            logger.error("JSchException", e);
            throw new LRMException("JSchException: " + e.getMessage());
        } catch (IOException e) {
            logger.error("IOException", e);
            throw new LRMException("IOException: " + e.getMessage());
        }
        return jobStatusMap;
    }

    public String getLSFHome() {
        return LSFHome;
    }

    public void setLSFHome(String lSFHome) {
        LSFHome = lSFHome;
    }

    public PBSSSHJob[] getJobs() {
        return jobs;
    }

    public void setJobs(PBSSSHJob[] jobs) {
        this.jobs = jobs;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

}
