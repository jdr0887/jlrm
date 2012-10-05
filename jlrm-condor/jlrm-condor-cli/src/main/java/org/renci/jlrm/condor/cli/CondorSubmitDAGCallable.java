package org.renci.jlrm.condor.cli;

import java.io.File;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.StringReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jgrapht.Graph;
import org.renci.common.exec.BashExecutor;
import org.renci.common.exec.CommandInput;
import org.renci.common.exec.CommandOutput;
import org.renci.common.exec.Executor;
import org.renci.common.exec.ExecutorException;
import org.renci.jlrm.AbstractSubmitCallable;
import org.renci.jlrm.JLRMException;
import org.renci.jlrm.condor.CondorJob;
import org.renci.jlrm.condor.CondorJobEdge;
import org.renci.jlrm.condor.CondorSubmitScriptExporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CondorSubmitDAGCallable extends AbstractSubmitCallable<CondorJob> {

    private final Logger logger = LoggerFactory.getLogger(CondorSubmitDAGCallable.class);

    private final Executor executor = BashExecutor.getInstance();

    private File submitDir;

    private Graph<CondorJob, CondorJobEdge> graph;

    private String dagName;

    private File condorHomeDirectory;

    public CondorSubmitDAGCallable() {
        super();
    }

    public CondorSubmitDAGCallable(File condorHomeDirectory, File submitDir, Graph<CondorJob, CondorJobEdge> graph,
            String dagName) {
        super();
        this.submitDir = submitDir;
        this.graph = graph;
        this.dagName = dagName;
        this.condorHomeDirectory = condorHomeDirectory;
    }

    @Override
    public CondorJob call() throws JLRMException {

        File workDir = createWorkDirectory(submitDir, this.dagName);
        CondorSubmitScriptExporter exporter = new CondorSubmitScriptExporter();
        CondorJob dagSubmitJob = exporter.export(dagName, workDir, graph);

        try {

            String command = String.format("%s/bin/condor_submit_dag %s", condorHomeDirectory.getAbsolutePath(),
                    dagSubmitJob.getSubmitFile().getName());
            CommandInput input = new CommandInput(command, dagSubmitJob.getSubmitFile().getParentFile());
            CommandOutput output = executor.execute(input);
            int exitCode = output.getExitCode();
            LineNumberReader lnr = new LineNumberReader(new StringReader(output.getStdout().toString()));
            logger.debug("executor.getStdout() = {}", output.getStdout().toString());
            String line;
            if (exitCode != 0) { // failed
                logger.debug("executor.getStderr() = {}", output.getStderr().toString());
                StringBuilder sb = new StringBuilder();
                while ((line = lnr.readLine()) != null) {
                    sb.append(String.format("%s%n", line));
                }
                lnr = new LineNumberReader(new StringReader(output.getStderr().toString()));
                while ((line = lnr.readLine()) != null) {
                    sb.append(String.format("%s%n", line));
                }
                logger.error(sb.toString());
                throw new JLRMException(sb.toString());
            }

            while ((line = lnr.readLine()) != null) {
                if (line.indexOf("submitted to cluster") != -1) {
                    logger.info("line = " + line);
                    Pattern pattern = Pattern.compile("(\\d*) job\\(s\\) submitted to cluster (\\d*)\\.");
                    Matcher matcher = pattern.matcher(line);
                    if (!matcher.matches()) {
                        throw new JLRMException("failed to parse the cluster number");
                    }
                    dagSubmitJob.setCluster(Integer.parseInt(matcher.group(2)));
                    // jobBean.setJobId(Integer.parseInt(matcher.group(1)));
                    dagSubmitJob.setJobId(0);
                    break;
                }
            }

            return dagSubmitJob;
        } catch (NumberFormatException e) {
            e.printStackTrace();
            throw new JLRMException("Failed to parse cluster id: " + e.getMessage());
        } catch (ExecutorException e) {
            e.printStackTrace();
            throw new JLRMException("ExecutorException: " + e.getMessage());
        } catch (IOException e) {
            e.printStackTrace();
            throw new JLRMException("IOException: " + e.getMessage());
        }

    }

    public File getSubmitDir() {
        return submitDir;
    }

    public void setSubmitDir(File submitDir) {
        this.submitDir = submitDir;
    }

    public Graph<CondorJob, CondorJobEdge> getGraph() {
        return graph;
    }

    public void setGraph(Graph<CondorJob, CondorJobEdge> graph) {
        this.graph = graph;
    }

    public String getDagName() {
        return dagName;
    }

    public void setDagName(String dagName) {
        this.dagName = dagName;
    }

}
