package org.renci.jlrm.commons.ssh;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.renci.jlrm.JLRMException;
import org.renci.jlrm.Site;
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
        logger.debug("ENTERING execute()");
        String ret = null;

        String home = System.getProperty("user.home");

        JSch.setLogger(new com.jcraft.jsch.Logger() {

            @Override
            public boolean isEnabled(int arg0) {
                return true;
            }

            @Override
            public void log(int level, String msg) {
                switch (level) {
                    case DEBUG:
                        // logger.debug(msg);
                        break;
                    case INFO:
                        // logger.info(msg);
                        break;
                    case WARN:
                        logger.warn(msg);
                        break;
                    case ERROR:
                    case FATAL:
                        logger.error(msg);
                        break;
                }
            }

        });
        JSch sch = new JSch();
        Session session = null;
        ChannelExec execChannel = null;
        ByteArrayOutputStream err = null;
        ByteArrayOutputStream out = null;
        try {
            sch.addIdentity(String.format("%s/.ssh/id_rsa", home));
            sch.setKnownHosts(String.format("%s/.ssh/known_hosts", home));
            session = sch.getSession(username, host, 22);
            session.setConfig("PreferredAuthentications", "publickey,keyboard-interactive,password");
            session.connect(30 * 1000);

            logger.debug("session.isConnected() = {}", session.isConnected());

            execChannel = (ChannelExec) session.openChannel("exec");
            execChannel.setInputStream(null);

            err = new ByteArrayOutputStream();
            execChannel.setErrStream(err);

            out = new ByteArrayOutputStream();
            execChannel.setOutputStream(out);

            execChannel.setCommand(command);

            InputStream in = execChannel.getInputStream();
            execChannel.connect(10 * 1000);

            logger.debug("execChannel.isConnected() = {}", execChannel.isConnected());

            do {
                Thread.sleep(1000);
            } while (!execChannel.isEOF());

            logger.info("command = {}", command);
            ret = IOUtils.toString(in).trim();

            int exitCode = execChannel.getExitStatus();
            logger.info("exitCode = {}", exitCode);

            if (exitCode != 0) {
                logger.warn("stdout: {}", new String(out.toByteArray()));
                logger.warn("stderr: {}", new String(err.toByteArray()));
            }

        } catch (InterruptedException e) {
            // e.printStackTrace();
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
                logger.debug("execChannel.isConnected() = {}", execChannel.isConnected());
            }
            if (session != null) {
                session.disconnect();
                logger.debug("session.isConnected() = {}", session.isConnected());
            }
        }
        return ret;
    }

    public static void transferInputs(Site site, String remoteWorkDir, List<File> inputFileList) throws JLRMException {
        transferInputs(site, remoteWorkDir, 0644, inputFileList);
    }

    public static void transferInputs(Site site, String remoteWorkDir, int mod, List<File> inputFileList)
            throws JLRMException {

        logger.debug("ENTERING transferSubmitScript()");

        String home = System.getProperty("user.home");
        String knownHostsFilename = home + "/.ssh/known_hosts";

        JSch sch = new JSch();
        Session session = null;
        ChannelSftp sftpChannel = null;

        try {
            sch.addIdentity(home + "/.ssh/id_rsa");
            sch.setKnownHosts(knownHostsFilename);
            session = sch.getSession(site.getUsername(), site.getSubmitHost(), 22);
            session.setConfig("PreferredAuthentications", "publickey,keyboard-interactive,password");
            session.connect(30000);

            logger.debug("session.isConnected() = {}", session.isConnected());

            sftpChannel = (ChannelSftp) session.openChannel("sftp");
            sftpChannel.connect(5 * 1000);

            logger.debug("sftpChannel.isConnected() = {}", sftpChannel.isConnected());

            sftpChannel.cd(remoteWorkDir);

            if (inputFileList != null && inputFileList.size() > 0) {
                for (File inputFile : inputFileList) {
                    sftpChannel.put(new FileInputStream(inputFile), inputFile.getName(), ChannelSftp.OVERWRITE);
                    sftpChannel.chmod(mod, inputFile.getName());
                }
            }

        } catch (Exception e) {
            logger.error("Exception", e);
            throw new JLRMException(e);
        } finally {
            if (sftpChannel != null) {
                sftpChannel.disconnect();
                logger.debug("sftpChannel.isConnected() = {}", sftpChannel.isConnected());
            }
            if (session != null) {
                session.disconnect();
                logger.debug("session.isConnected() = {}", session.isConnected());
            }
        }

    }

    public static void transferSubmitScript(Site site, String remoteWorkDir, boolean transferExecutable,
            File executable, boolean transferInputs, List<File> inputFileList, File submitFile) throws JLRMException {
        logger.debug("ENTERING transferSubmitScript()");

        String home = System.getProperty("user.home");
        String knownHostsFilename = home + "/.ssh/known_hosts";

        JSch sch = new JSch();
        Session session = null;
        ChannelSftp sftpChannel = null;

        try {
            sch.addIdentity(home + "/.ssh/id_rsa");
            sch.setKnownHosts(knownHostsFilename);
            session = sch.getSession(site.getUsername(), site.getSubmitHost(), 22);
            session.setConfig("PreferredAuthentications", "publickey,keyboard-interactive,password");
            session.connect(30000);

            logger.debug("session.isConnected() = {}", session.isConnected());

            sftpChannel = (ChannelSftp) session.openChannel("sftp");
            sftpChannel.connect(5 * 1000);

            logger.debug("sftpChannel.isConnected() = {}", sftpChannel.isConnected());

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
                logger.debug("sftpChannel.isConnected() = {}", sftpChannel.isConnected());
            }
            if (session != null) {
                session.disconnect();
                logger.debug("session.isConnected() = {}", session.isConnected());
            }
        }

    }

    public static void transferOutputs(Site site, String remoteWorkDir, File destinationDir, String... outputs)
            throws JLRMException {
        logger.debug("ENTERING transferSubmitScript()");

        String home = System.getProperty("user.home");
        String knownHostsFilename = home + "/.ssh/known_hosts";

        JSch sch = new JSch();
        Session session = null;
        ChannelSftp sftpChannel = null;

        try {
            sch.addIdentity(home + "/.ssh/id_rsa");
            sch.setKnownHosts(knownHostsFilename);
            session = sch.getSession(site.getUsername(), site.getSubmitHost(), 22);
            session.setConfig("PreferredAuthentications", "publickey,keyboard-interactive,password");
            session.connect(30000);

            logger.debug("session.isConnected() = {}", session.isConnected());

            sftpChannel = (ChannelSftp) session.openChannel("sftp");
            sftpChannel.connect(5 * 1000);

            logger.debug("sftpChannel.isConnected() = {}", sftpChannel.isConnected());

            sftpChannel.cd(remoteWorkDir);

            for (String output : Arrays.asList(outputs)) {
                sftpChannel.get(output, String.format("%s/%s", destinationDir.getAbsolutePath(), output));
            }

        } catch (JSchException | SftpException e) {
            logger.error(e.getMessage(), e);
            throw new JLRMException(e);
        } finally {
            if (sftpChannel != null) {
                sftpChannel.disconnect();
                logger.debug("sftpChannel.isConnected() = {}", sftpChannel.isConnected());
            }
            if (session != null) {
                session.disconnect();
                logger.debug("session.isConnected() = {}", session.isConnected());
            }
        }

    }

}
