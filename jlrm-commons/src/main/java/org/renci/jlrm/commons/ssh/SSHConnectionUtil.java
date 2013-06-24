package org.renci.jlrm.commons.ssh;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.renci.jlrm.JLRMException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;

public class SSHConnectionUtil {

    private static final Logger logger = LoggerFactory.getLogger(SSHConnectionUtil.class);

    public static String execute(String command, String username, String host) throws JLRMException {
        logger.info("ENTERING execute()");
        String ret = null;

        String home = System.getProperty("user.home");
        String knownHostsFilename = home + "/.ssh/known_hosts";

        JSch sch = new JSch();
        Session session = null;
        ChannelExec execChannel = null;
        ByteArrayOutputStream err = null;
        ByteArrayOutputStream out = null;
        try {
            sch.addIdentity(home + "/.ssh/id_rsa");
            sch.setKnownHosts(knownHostsFilename);
            session = sch.getSession(username, host, 22);
            Properties config = new Properties();
            config.setProperty("StrictHostKeyChecking", "no");
            session.setConfig(config);
            session.connect(30000);

            execChannel = (ChannelExec) session.openChannel("exec");
            execChannel.setInputStream(null);

            err = new ByteArrayOutputStream();
            execChannel.setErrStream(err);

            out = new ByteArrayOutputStream();
            execChannel.setOutputStream(out);

            execChannel.setCommand(command);

            InputStream in = execChannel.getInputStream();
            execChannel.connect(5 * 1000);

            ret = IOUtils.toString(in).trim();
            int exitCode = execChannel.getExitStatus();
            logger.info("exitCode = {}", exitCode);

        } catch (JSchException e) {
            logger.error("JSchException", e);
            throw new JLRMException("JSchException: " + e.getMessage());
        } catch (IOException e) {
            logger.error("IOException", e);
            throw new JLRMException("IOException: " + e.getMessage());
        } finally {
            try {
                if (err != null) {
                    err.close();
                }
                if (out != null) {
                    out.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

            if (execChannel != null) {
                execChannel.disconnect();
            }
            if (session != null) {
                session.disconnect();
            }
        }
        return ret;
    }

    public static void transferSubmitScript(String username, String host, String remoteWorkDir,
            boolean transferExecutable, File executable, boolean transferInputs, List<File> inputFileList,
            File submitFile) throws JLRMException {
        logger.info("ENTERING transferSubmitScript()");

        String home = System.getProperty("user.home");
        String knownHostsFilename = home + "/.ssh/known_hosts";

        JSch sch = new JSch();
        Session session = null;
        ChannelSftp sftpChannel = null;

        try {
            sch.addIdentity(home + "/.ssh/id_rsa");
            sch.setKnownHosts(knownHostsFilename);
            session = sch.getSession(username, host, 22);
            Properties config = new Properties();
            config.setProperty("StrictHostKeyChecking", "no");
            session.setConfig(config);
            session.connect(30000);

            sftpChannel = (ChannelSftp) session.openChannel("sftp");
            sftpChannel.connect(5 * 1000);
            sftpChannel.cd(remoteWorkDir);

            if (transferExecutable) {
                sftpChannel.put(new FileInputStream(executable), executable.getName(), ChannelSftp.OVERWRITE);
                sftpChannel.chmod(0755, executable.getName());
            }

            if (transferInputs && inputFileList != null && inputFileList.size() > 0) {
                for (File inputFile : inputFileList) {
                    sftpChannel.put(new FileInputStream(inputFile), inputFile.getName(), ChannelSftp.OVERWRITE);
                    sftpChannel.chmod(0644, inputFile.getName());
                }
            }
            sftpChannel.put(new FileInputStream(submitFile), submitFile.getName(), ChannelSftp.OVERWRITE);
            sftpChannel.chmod(0644, submitFile.getName());
            sftpChannel.disconnect();
        } catch (FileNotFoundException e) {
            logger.error("FileNotFoundException", e);
            throw new JLRMException("FileNotFoundException: " + e.getMessage());
        } catch (JSchException e) {
            logger.error("JSchException", e);
            throw new JLRMException("JSchException: " + e.getMessage());
        } catch (SftpException e) {
            logger.error("SftpException", e);
            throw new JLRMException("SftpException: " + e.getMessage());
        } finally {
            if (sftpChannel != null) {
                sftpChannel.disconnect();
            }
            if (session != null) {
                session.disconnect();
            }
        }

    }

}
