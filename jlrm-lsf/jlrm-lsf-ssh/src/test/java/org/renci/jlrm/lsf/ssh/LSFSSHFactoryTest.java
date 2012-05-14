package org.renci.jlrm.lsf.ssh;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.LineNumberReader;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.junit.Test;
import org.renci.jlrm.LRMException;
import org.renci.jlrm.lsf.LSFJobStatusType;

import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

public class LSFSSHFactoryTest {

    public LSFSSHFactoryTest() {
        super();
    }

    @Test
    public void testBasicSubmit() {

        LSFSSHFactory lsfSSHFactory = LSFSSHFactory.getInstance(
                "/nas02/apps/lsf/LSF_TOP_706/7.0/linux2.6-glibc2.3-x86_64/", "jreilly", "biodev2.its.unc.edu");

        LSFSSHJob job = new LSFSSHJob("test", new File("/bin/hostname"));
        job.setHostCount(1);
        job.setNumberOfProcessors(1);
        job.setProject("TCGA");
        job.setQueueName("week");
        job.setOutput(new File("test.out"));
        job.setError(new File("test.err"));

        try {
            job = lsfSSHFactory.submit(new File("/tmp"), job);
            System.out.println(job.getId());
        } catch (LRMException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testGlideinSubmit() {

        LSFSSHFactory lsfSSHFactory = LSFSSHFactory.getInstance(
                "/nas02/apps/lsf/LSF_TOP_706/7.0/linux2.6-glibc2.3-x86_64/", "jreilly", "biodev2.its.unc.edu");
        File submitDir = new File("/tmp");

        try {
            // LSFSSHJob job = lsfSSHFactory.submitGlidein(submitDir, 2, 30, 40, "biodev1.its.unc.edu", "idle");
            // LSFSSHJob job = lsfSSHFactory.submitGlidein(submitDir, 2, 30, 40, "biodev1.its.unc.edu", "debug");
            // LSFSSHJob job = lsfSSHFactory.submitGlidein(submitDir, 2, 30, 40, "biodev1.its.unc.edu", "huge");
            // LSFSSHJob job = lsfSSHFactory.submitGlidein(submitDir, 2, 30, 40, "biodev1.its.unc.edu", "week");
            LSFSSHJob job = lsfSSHFactory.submitGlidein(submitDir, 2, 30, 40, "biodev1.its.unc.edu", "pseq_prod");
            System.out.println(job.getId());
        } catch (LRMException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testLookupStatus() {

        String command = String.format("%s/bin/bjobs %s | tail -n+2 | awk '{print $1,$3}'",
                "/nas02/apps/lsf/LSF_TOP_706/7.0/linux2.6-glibc2.3-x86_64/", "173198 173244");

        String home = System.getProperty("user.home");
        String knownHostsFilename = home + "/.ssh/known_hosts";

        JSch sch = new JSch();
        try {
            sch.addIdentity(home + "/.ssh/id_rsa");
            sch.setKnownHosts(knownHostsFilename);
            Session session = sch.getSession("jreilly", "biodev1.its.unc.edu", 22);
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

            Map<String, LSFJobStatusType> jobStatusMap = new HashMap<String, LSFJobStatusType>();
            LineNumberReader lnr = new LineNumberReader(new StringReader(output));
            String line;
            while ((line = lnr.readLine()) != null) {
                LSFJobStatusType statusType = LSFJobStatusType.DONE;
                if (StringUtils.isNotEmpty(line)) {
                    if (line.contains("is not found")) {
                        statusType = LSFJobStatusType.DONE;
                    } else {
                        //System.out.println(line);
                        String[] lineSplit = line.split(" ");
                        if (lineSplit != null && lineSplit.length == 2) {
                            for (LSFJobStatusType type : LSFJobStatusType.values()) {
                                if (type.getValue().equals(lineSplit[1])) {
                                    statusType = type;
                                }
                            }
                            jobStatusMap.put(lineSplit[0], statusType);
                        }
                    }
                }
            }

            for (String id : jobStatusMap.keySet()) {
                System.out.println("Job: " + id + " has a status of " + jobStatusMap.get(id));
            }
            err.close();
            out.close();
            session.disconnect();
        } catch (JSchException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
