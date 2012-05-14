package org.renci.jlrm.lsf.ssh;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
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

        LSFJobStatusType ret = LSFJobStatusType.UNKNOWN;
        String command = String.format("%s/bin/bjobs %s | tail -n+2 | awk '{print $3}'",
                "/nas02/apps/lsf/LSF_TOP_706/7.0/linux2.6-glibc2.3-x86_64/", "910582");

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
            String status = IOUtils.toString(in).trim();
            int exitCode = execChannel.getExitStatus();
            execChannel.disconnect();

            if (exitCode != 0) {
                String error = new String(err.toByteArray());
            } else {

                if (StringUtils.isNotEmpty(status)) {
                    if (status.contains("is not found")) {
                        ret = LSFJobStatusType.DONE;
                    } else {
                        for (LSFJobStatusType type : LSFJobStatusType.values()) {
                            if (type.getValue().equals(status)) {
                                ret = type;
                            }
                        }
                    }
                } else {
                    ret = LSFJobStatusType.DONE;
                }

            }
        } catch (JSchException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("ret = " + ret.getValue());
    }

}
