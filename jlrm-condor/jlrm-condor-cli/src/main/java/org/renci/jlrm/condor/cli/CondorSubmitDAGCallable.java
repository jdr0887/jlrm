package org.renci.jlrm.condor.cli;

import java.io.File;
import java.io.LineNumberReader;
import java.io.StringReader;
import java.nio.file.Path;
import java.util.concurrent.Callable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.renci.common.exec.BashExecutor;
import org.renci.common.exec.CommandInput;
import org.renci.common.exec.CommandOutput;
import org.renci.common.exec.Executor;
import org.renci.jlrm.JLRMException;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@Slf4j
public class CondorSubmitDAGCallable implements Callable<Integer> {

    private Path dagSubmitScript;

    private Boolean dryRun;

    public CondorSubmitDAGCallable(Path dagSubmitScript) {
        super();
        this.dagSubmitScript = dagSubmitScript;
    }

    @Override
    public Integer call() throws Exception {

        try {

            String command = String.format("condor_submit_dag -force %s", dagSubmitScript.toAbsolutePath().toString());
            CommandInput input = new CommandInput(command, dagSubmitScript.getParent().toFile());
            input.setExitImmediately(Boolean.FALSE);
            Executor executor = BashExecutor.getInstance();
            CommandOutput output = executor.execute(input, new File(System.getProperty("user.home"), ".bashrc"));
            int exitCode = output.getExitCode();
            LineNumberReader lnr = new LineNumberReader(new StringReader(output.getStdout().toString()));
            log.debug("executor.getStdout() = {}", output.getStdout().toString());
            String line;
            if (exitCode != 0) { // failed
                log.debug("executor.getStderr() = {}", output.getStderr().toString());
                StringBuilder sb = new StringBuilder();
                while ((line = lnr.readLine()) != null) {
                    sb.append(String.format("%s%n", line));
                }
                lnr = new LineNumberReader(new StringReader(output.getStderr().toString()));
                while ((line = lnr.readLine()) != null) {
                    sb.append(String.format("%s%n", line));
                }
                log.error(sb.toString());
                throw new JLRMException(sb.toString());
            }

            Integer ret = null;
            while ((line = lnr.readLine()) != null) {
                if (line.indexOf("submitted to cluster") != -1) {
                    log.info("line = " + line);
                    Pattern pattern = Pattern.compile("(\\d*) job\\(s\\) submitted to cluster (\\d*)\\.");
                    Matcher matcher = pattern.matcher(line);
                    if (!matcher.matches()) {
                        throw new JLRMException("failed to parse the cluster number");
                    }
                    ret = Integer.parseInt(matcher.group(2));
                    break;
                }
            }

            return ret;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw e;
        }

    }

}
