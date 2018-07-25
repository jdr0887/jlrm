package org.renci.jlrm.slurm.ssh;

import java.util.concurrent.Callable;

import org.renci.jlrm.JLRMException;
import org.renci.jlrm.Site;
import org.renci.jlrm.commons.ssh.SSHConnectionUtil;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@AllArgsConstructor
@Getter
@Setter
@NoArgsConstructor
public class SLURMSSHKillCallable implements Callable<Void> {

    private Site site;

    private String jobId;

    @Override
    public Void call() throws JLRMException {
        String command = String.format("scancel %s", jobId);
        SSHConnectionUtil.execute(command, site.getUsername(), getSite().getSubmitHost());
        return null;
    }

}
