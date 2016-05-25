package org.renci.jlrm.condor.ext;

import static org.renci.jlrm.condor.ClassAdvertisementFactory.CLASS_AD_KEY_ERROR;
import static org.renci.jlrm.condor.ClassAdvertisementFactory.CLASS_AD_KEY_EXECUTABLE;
import static org.renci.jlrm.condor.ClassAdvertisementFactory.CLASS_AD_KEY_LOG;
import static org.renci.jlrm.condor.ClassAdvertisementFactory.CLASS_AD_KEY_OUTPUT;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.jgrapht.Graph;
import org.renci.jlrm.condor.ClassAdvertisement;
import org.renci.jlrm.condor.ClassAdvertisementFactory;
import org.renci.jlrm.condor.CondorJob;
import org.renci.jlrm.condor.CondorJobEdge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CondorSubmitScriptExporter {

    private static final Logger logger = LoggerFactory.getLogger(CondorSubmitScriptExporter.class);

    public CondorSubmitScriptExporter() {
        super();
    }

    public CondorJob export(File workDir, CondorJob job) {
        try {

            ClassAdvertisement classAd = ClassAdvertisementFactory.getClassAd(CLASS_AD_KEY_EXECUTABLE).clone();
            classAd.setValue(job.getExecutable().getAbsolutePath());
            job.getClassAdvertisments().add(classAd);

            classAd = ClassAdvertisementFactory.getClassAd(CLASS_AD_KEY_OUTPUT).clone();
            classAd.setValue(String.format("%s.out", job.getName()));
            job.getClassAdvertisments().add(classAd);

            classAd = ClassAdvertisementFactory.getClassAd(CLASS_AD_KEY_ERROR).clone();
            classAd.setValue(String.format("%s.err", job.getName()));
            job.getClassAdvertisments().add(classAd);

            classAd = ClassAdvertisementFactory.getClassAd(CLASS_AD_KEY_LOG).clone();
            classAd.setValue(String.format("%s.log", job.getName()));
            job.getClassAdvertisments().add(classAd);

            File submitFile = writeSubmitFile(workDir, job);
            job.setSubmitFile(submitFile);
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return job;
    }

    public CondorJob export(String dagName, File workDir, Graph<CondorJob, CondorJobEdge> graph) {
        logger.debug("ENTERING export(String dagName, File workDir, Graph<JobNode, JobEdge> graph)");
        return export(dagName, workDir, graph, true);
    }

    public CondorJob export(String dagName, File workDir, Graph<CondorJob, CondorJobEdge> graph,
            boolean includeGlideinRequirements) {
        logger.debug("ENTERING export(String dagName, File workDir, Graph<JobNode, JobEdge> graph)");

        CondorJob dagSubmitJob = new CondorJob();
        dagSubmitJob.setName(dagName);
        File dagFile = new File(workDir, dagName + ".dag");
        dagSubmitJob.setSubmitFile(dagFile);

        try {

            if (graph != null && graph.vertexSet().size() > 0) {

                ClassAdvertisement classAd = null;
                for (CondorJob job : graph.vertexSet()) {

                    classAd = ClassAdvertisementFactory.getClassAd(CLASS_AD_KEY_EXECUTABLE).clone();
                    classAd.setValue(job.getExecutable().getAbsolutePath());
                    job.getClassAdvertisments().add(classAd);

                    classAd = ClassAdvertisementFactory.getClassAd(CLASS_AD_KEY_OUTPUT).clone();
                    classAd.setValue(String.format("%s.out", job.getName()));
                    job.getClassAdvertisments().add(classAd);

                    classAd = ClassAdvertisementFactory.getClassAd(CLASS_AD_KEY_ERROR).clone();
                    classAd.setValue(String.format("%s.err", job.getName()));
                    job.getClassAdvertisments().add(classAd);

                    classAd = ClassAdvertisementFactory.getClassAd(CLASS_AD_KEY_LOG).clone();
                    classAd.setValue(String.format("%s.log", job.getName()));
                    job.getClassAdvertisments().add(classAd);

                    if (includeGlideinRequirements) {
                        job.addRequirement(String.format("TARGET.JLRM_USER == \"%s\"", System.getProperty("user.name")));
                        job.addRequirement("TARGET.IS_GLIDEIN == True");
                    }

                }

                FileWriter dagFileWriter = new FileWriter(dagFile);

                for (CondorJob job : graph.vertexSet()) {
                    writeSubmitFile(workDir, job);
                    dagFileWriter.write(String.format("%n%1$-10s %2$-10s %2$s.sub", "JOB", job.getName()));
                    if (StringUtils.isNotEmpty(job.getPreScript())) {
                        dagFileWriter.write(String.format("%n%1$-10s %2$-10s %3$-10s %4$-10s", "SCRIPT", "PRE",
                                job.getName(), job.getPreScript()));
                    }
                    if (StringUtils.isNotEmpty(job.getPostScript())) {
                        dagFileWriter.write(String.format("%n%1$-10s %2$-10s %3$-10s %4$-10s", "SCRIPT", "POST",
                                job.getName(), job.getPostScript()));
                    }
                    if (job.getRetry() != null && job.getRetry() > 1) {
                        dagFileWriter.write(String.format("%n%1$-10s %2$-10s %3$d%n", "RETRY", job.getName(),
                                job.getRetry()));
                    }
                    dagFileWriter.flush();
                }

                dagFileWriter.write(System.getProperty("line.separator"));

                for (CondorJobEdge edge : graph.edgeSet()) {
                    CondorJob source = (CondorJob) edge.getSource();
                    CondorJob target = (CondorJob) edge.getTarget();
                    String format = "%1$-10s %2$-10s %3$-10s %4$s%n";
                    dagFileWriter.write(String.format(format, "PARENT", source.getName(), "CHILD", target.getName()));
                    dagFileWriter.flush();
                }

                dagFileWriter.close();
            }

        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        } catch (IOException e1) {
            e1.printStackTrace();
        }

        return dagSubmitJob;
    }

    protected File writeSubmitFile(File submitDir, CondorJob job) throws IOException {
        File submitFile = new File(submitDir, String.format("%s.sub", job.getName()));
        FileWriter submitFileWriter = new FileWriter(submitFile);
        for (ClassAdvertisement classAd : job.getClassAdvertisments()) {
            switch (classAd.getType()) {
                case BOOLEAN:
                case EXPRESSION:
                case INTEGER:
                    submitFileWriter.write(String.format("%1$-25s = %2$s%n", classAd.getKey(), classAd.getValue()));
                    break;
                case STRING:
                default:
                    submitFileWriter.write(String.format("%1$-25s = \"%2$s\"%n", classAd.getKey(), classAd.getValue()));
                    break;
            }
            submitFileWriter.flush();
        }
        submitFileWriter.write(String.format("%s%n",
                ClassAdvertisementFactory.getClassAd(ClassAdvertisementFactory.CLASS_AD_KEY_QUEUE).getKey()));
        submitFileWriter.flush();
        submitFileWriter.close();
        return submitFile;
    }

}