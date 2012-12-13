package org.renci.jlrm.lsf.ssh;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyPair;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.connection.channel.direct.Session;
import net.schmizz.sshj.connection.channel.direct.Session.Command;
import net.schmizz.sshj.transport.TransportException;
import net.schmizz.sshj.userauth.UserAuthException;

import org.apache.commons.io.IOUtils;
import org.apache.sshd.ClientChannel;
import org.apache.sshd.ClientSession;
import org.apache.sshd.SshClient;
import org.apache.sshd.common.KeyPairProvider;
import org.apache.sshd.common.keyprovider.FileKeyPairProvider;
import org.junit.Test;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;

public class SSHConnectionTest {

    public SSHConnectionTest() {
        super();
    }

    @Test
    public void testExecSSHJ() {

        final SSHClient ssh = new SSHClient();
        try {
            ssh.loadKnownHosts();
            ssh.connect("biodev1.its.unc.edu");
            ssh.authPublickey("jreilly", System.getProperty("user.home") + "/.ssh/id_rsa");

            Session session = ssh.startSession();
            final Command command = session.exec("/bin/echo hello");
            String commandOutput = net.schmizz.sshj.common.IOUtils.readFully(command.getInputStream()).toString()
                    .trim();
            System.out.println(commandOutput);
            command.join(5, TimeUnit.SECONDS);
            session.close();
            ssh.close();
        } catch (UserAuthException e) {
            e.printStackTrace();
        } catch (TransportException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testExecSSHCore() {
        String home = System.getProperty("user.home");
        SshClient sshClient = SshClient.setUpDefaultClient();
        sshClient.start();
        try {
            ClientSession session = sshClient.connect("biodev1.its.unc.edu", 22).await().getSession();
            KeyPair pair = new FileKeyPairProvider(new String[] { home + "/.ssh/id_rsa" })
                    .loadKey(KeyPairProvider.SSH_RSA);
            session.authPublicKey("jreilly", pair);

            ClientChannel channel = session.createExecChannel("/bin/echo hello");

            channel.setIn(new ByteArrayInputStream(new byte[0]));

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            channel.setOut(out);

            ByteArrayOutputStream err = new ByteArrayOutputStream();
            channel.setErr(err);

            channel.open();
            channel.waitFor(ClientChannel.CLOSED, 0);

            session.close(true);
            System.out.println(new String(out.toByteArray()));
            System.out.println(new String(err.toByteArray()));
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testScpJSCH() {
        String home = System.getProperty("user.home");
        File root = new File(home, "/tiny");
        String knownHostsFilename = home + "/.ssh/known_hosts";
        try {
            JSch sch = new JSch();
            sch.addIdentity(home + "/.ssh/id_rsa");
            sch.setKnownHosts(knownHostsFilename);
            com.jcraft.jsch.Session session = sch.getSession("jreilly", "biodev1.its.unc.edu", 22);
            Properties config = new Properties();
            config.setProperty("StrictHostKeyChecking", "no");
            config.setProperty("compression.s2c", "zlib,none");
            config.setProperty("compression.c2s", "zlib,none");
            config.setProperty("compression_level", "9");
            session.setConfig(config);
            session.connect(30000);
            com.jcraft.jsch.ChannelSftp channel = (com.jcraft.jsch.ChannelSftp) session.openChannel("sftp");
            channel.connect();
            channel.cd("/tmp");
            channel.put(new FileInputStream(root), "tiny", ChannelSftp.OVERWRITE);
            channel.chmod(0755, "tiny");
            channel.disconnect();
            session.disconnect();
        } catch (SftpException e) {
            e.printStackTrace();
        } catch (JSchException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testExecJSCH() {
        String home = System.getProperty("user.home");
        String knownHostsFilename = home + "/.ssh/known_hosts";
        try {
            JSch sch = new JSch();
            sch.addIdentity(home + "/.ssh/id_rsa");
            sch.setKnownHosts(knownHostsFilename);
            com.jcraft.jsch.Session session = sch.getSession("jreilly", "biodev1.its.unc.edu", 22);
            Properties config = new Properties();
            config.setProperty("StrictHostKeyChecking", "no");
            session.setConfig(config);
            session.connect(30000);
            com.jcraft.jsch.ChannelExec channel = (com.jcraft.jsch.ChannelExec) session.openChannel("exec");
            channel.setInputStream(null);

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            channel.setOutputStream(out);

            ByteArrayOutputStream err = new ByteArrayOutputStream();
            channel.setErrStream(err);
            String command = String.format("mkdir -p $HOME/%s && echo $HOME", "asdfasdf");
            // String command = "/bin/echo hello";

            channel.setCommand(command);
            InputStream in = channel.getInputStream();
            channel.connect();
            String remoteHome = IOUtils.toString(in);
            StringBuilder sb = new StringBuilder();
            sb.append(remoteHome.trim()).append("/asdf");
            System.out.println(sb.toString());
            channel.disconnect();
            session.disconnect();
            System.out.println("-------------");
            System.out.println(new String(out.toByteArray()));
            System.out.println("-------------");
            System.out.println(new String(err.toByteArray()));
        } catch (JSchException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
