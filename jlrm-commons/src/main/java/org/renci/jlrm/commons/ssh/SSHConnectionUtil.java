package org.renci.jlrm.commons.ssh;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.renci.jlrm.JLRMException;
import org.renci.jlrm.Site;

import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SSHConnectionUtil {

    public static String execute(String command, String username, String host) throws JLRMException {
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
                        log.warn(msg);
                        break;
                    case ERROR:
                    case FATAL:
                        log.error(msg);
                        break;
                }
            }

        });
        JSch sch = new JSch();
        Session session = null;
        ChannelExec execChannel = null;

        try (ByteArrayOutputStream err = new ByteArrayOutputStream();
                ByteArrayOutputStream out = new ByteArrayOutputStream()) {

            sch.addIdentity(String.format("%s/.ssh/id_rsa", home));
            sch.setKnownHosts(String.format("%s/.ssh/known_hosts", home));
            session = sch.getSession(username, host, 22);
            session.setConfig("PreferredAuthentications", "publickey,keyboard-interactive,password");
            session.connect(30 * 1000);

            log.debug("session.isConnected() = {}", session.isConnected());

            execChannel = (ChannelExec) session.openChannel("exec");
            execChannel.setInputStream(null);

            execChannel.setErrStream(err);
            execChannel.setOutputStream(out);

            log.info("command: {}", command);
            execChannel.setCommand(command);

            InputStream in = execChannel.getInputStream();
            execChannel.connect(10 * 1000);

            log.debug("execChannel.isConnected() = {}", execChannel.isConnected());

            do {
                Thread.sleep(1000);
            } while (!execChannel.isEOF());

            log.info("command = {}", command);
            ret = IOUtils.toString(in).trim();

            int exitCode = execChannel.getExitStatus();
            log.info("exitCode = {}", exitCode);

            if (exitCode != 0) {
                log.warn("stdout: {}", new String(out.toByteArray()));
                log.warn("stderr: {}", new String(err.toByteArray()));
            }

        } catch (InterruptedException e) {
            // e.printStackTrace();
        } catch (JSchException e) {
            log.error("JSchException", e);
            throw new JLRMException("JSchException: " + e.getMessage());
        } catch (IOException e) {
            log.error("IOException", e);
            throw new JLRMException("IOException: " + e.getMessage());
        } finally {

            if (execChannel != null) {
                execChannel.disconnect();
                log.debug("execChannel.isConnected() = {}", execChannel.isConnected());
            }
            if (session != null) {
                session.disconnect();
                log.debug("session.isConnected() = {}", session.isConnected());
            }
        }
        return ret;
    }

    public static void transferInputs(Site site, String remoteWorkDir, List<File> inputFileList) throws JLRMException {
        transferInputs(site, remoteWorkDir, 0644, inputFileList);
    }

    public static void transferInputs(Site site, String remoteWorkDir, int mod, List<File> inputFileList)
            throws JLRMException {

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

            log.debug("session.isConnected() = {}", session.isConnected());

            sftpChannel = (ChannelSftp) session.openChannel("sftp");
            sftpChannel.connect(5 * 1000);

            log.debug("sftpChannel.isConnected() = {}", sftpChannel.isConnected());

            sftpChannel.cd(remoteWorkDir);

            if (inputFileList != null && inputFileList.size() > 0) {
                for (File inputFile : inputFileList) {
                    sftpChannel.put(new FileInputStream(inputFile), inputFile.getName(), ChannelSftp.OVERWRITE);
                    sftpChannel.chmod(mod, inputFile.getName());
                }
            }

        } catch (Exception e) {
            log.error("Exception", e);
            throw new JLRMException(e);
        } finally {
            if (sftpChannel != null) {
                sftpChannel.disconnect();
                log.debug("sftpChannel.isConnected() = {}", sftpChannel.isConnected());
            }
            if (session != null) {
                session.disconnect();
                log.debug("session.isConnected() = {}", session.isConnected());
            }
        }

    }

    public static void transferSubmitScript(Site site, String remoteWorkDir, boolean transferExecutable,
            Path executable, boolean transferInputs, List<File> inputFileList, Path submitFile) throws JLRMException {
        log.debug("ENTERING transferSubmitScript()");

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

            log.debug("session.isConnected() = {}", session.isConnected());

            sftpChannel = (ChannelSftp) session.openChannel("sftp");
            sftpChannel.connect(5 * 1000);

            log.debug("sftpChannel.isConnected() = {}", sftpChannel.isConnected());

            sftpChannel.cd(remoteWorkDir);

            if (transferExecutable) {
                sftpChannel.put(new FileInputStream(executable.toFile()), executable.getFileName().toString(),
                        ChannelSftp.OVERWRITE);
                sftpChannel.chmod(0755, executable.getFileName().toString());
            }

            if (transferInputs && inputFileList != null && inputFileList.size() > 0) {
                for (File inputFile : inputFileList) {
                    sftpChannel.put(new FileInputStream(inputFile), inputFile.getName(), ChannelSftp.OVERWRITE);
                    sftpChannel.chmod(0644, inputFile.getName());
                }
            }

            sftpChannel.put(new FileInputStream(submitFile.toFile()), submitFile.getFileName().toString(),
                    ChannelSftp.OVERWRITE);
            sftpChannel.chmod(0644, submitFile.getFileName().toString());

        } catch (FileNotFoundException e) {
            log.error("FileNotFoundException", e);
            throw new JLRMException("FileNotFoundException: " + e.getMessage());
        } catch (JSchException e) {
            log.error("JSchException", e);
            throw new JLRMException("JSchException: " + e.getMessage());
        } catch (SftpException e) {
            log.error("SftpException", e);
            throw new JLRMException("SftpException: " + e.getMessage());
        } finally {
            if (sftpChannel != null) {
                sftpChannel.disconnect();
                log.debug("sftpChannel.isConnected() = {}", sftpChannel.isConnected());
            }
            if (session != null) {
                session.disconnect();
                log.debug("session.isConnected() = {}", session.isConnected());
            }
        }

    }

    public static void transferOutputs(Site site, String remoteWorkDir, File destinationDir, String... outputs)
            throws JLRMException {
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

            log.debug("session.isConnected() = {}", session.isConnected());

            sftpChannel = (ChannelSftp) session.openChannel("sftp");
            sftpChannel.connect(5 * 1000);

            log.debug("sftpChannel.isConnected() = {}", sftpChannel.isConnected());

            sftpChannel.cd(remoteWorkDir);

            for (String output : Arrays.asList(outputs)) {
                sftpChannel.get(output, String.format("%s/%s", destinationDir.getAbsolutePath(), output));
            }

        } catch (JSchException | SftpException e) {
            log.error(e.getMessage(), e);
            throw new JLRMException(e);
        } finally {
            if (sftpChannel != null) {
                sftpChannel.disconnect();
                log.debug("sftpChannel.isConnected() = {}", sftpChannel.isConnected());
            }
            if (session != null) {
                session.disconnect();
                log.debug("session.isConnected() = {}", session.isConnected());
            }
        }

    }

}
