package org.renci.jlrm.condor;

import static org.renci.jlrm.condor.ClassAdvertisementFactory.CLASS_AD_KEY_ERROR;
import static org.renci.jlrm.condor.ClassAdvertisementFactory.CLASS_AD_KEY_EXECUTABLE;
import static org.renci.jlrm.condor.ClassAdvertisementFactory.CLASS_AD_KEY_LOG;
import static org.renci.jlrm.condor.ClassAdvertisementFactory.CLASS_AD_KEY_OUTPUT;
import static org.renci.jlrm.condor.ClassAdvertisementFactory.CLASS_AD_KEY_REQUEST_CPUS;
import static org.renci.jlrm.condor.ClassAdvertisementFactory.CLASS_AD_KEY_REQUEST_MEMORY;
import static org.renci.jlrm.condor.ClassAdvertisementFactory.CLASS_AD_KEY_REQUIREMENTS;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.jgrapht.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CondorSubmitScriptExporter {

    private final Logger logger = LoggerFactory.getLogger(CondorSubmitScriptExporter.class);

    private static CondorSubmitScriptExporter instance;

    public static CondorSubmitScriptExporter getInstance() {
        if (instance == null) {
            instance = new CondorSubmitScriptExporter();
        }
        return instance;
    }

    private CondorSubmitScriptExporter() {
        super();
    }

    public CondorJob export(File workDir, CondorJob job) {
        try {

            ClassAdvertisement classAd = ClassAdvertisementFactory.getClassAd(CLASS_AD_KEY_EXECUTABLE).clone();
            classAd.setValue(job.getExecutable().getPath());
            job.getClassAdvertismentMap().put(CLASS_AD_KEY_EXECUTABLE, classAd);

            classAd = ClassAdvertisementFactory.getClassAd(CLASS_AD_KEY_OUTPUT).clone();
            classAd.setValue(String.format("%s/%s.out", workDir.getAbsolutePath(), job.getName()));
            job.getClassAdvertismentMap().put(CLASS_AD_KEY_OUTPUT, classAd);

            classAd = ClassAdvertisementFactory.getClassAd(CLASS_AD_KEY_ERROR).clone();
            classAd.setValue(String.format("%s/%s.err", workDir.getAbsolutePath(), job.getName()));
            job.getClassAdvertismentMap().put(CLASS_AD_KEY_ERROR, classAd);

            classAd = ClassAdvertisementFactory.getClassAd(CLASS_AD_KEY_LOG).clone();
            classAd.setValue(String.format("%s/%s.log", workDir.getAbsolutePath(), job.getName()));
            job.getClassAdvertismentMap().put(CLASS_AD_KEY_LOG, classAd);

            classAd = ClassAdvertisementFactory.getClassAd(CLASS_AD_KEY_REQUEST_CPUS).clone();
            classAd.setValue(String.format("%d", job.getNumberOfProcessors()));
            job.getClassAdvertismentMap().put(CLASS_AD_KEY_REQUEST_CPUS, classAd);

            classAd = ClassAdvertisementFactory.getClassAd(CLASS_AD_KEY_REQUEST_MEMORY).clone();
            classAd.setValue(String.format("%d", job.getMemory()));
            job.getClassAdvertismentMap().put(CLASS_AD_KEY_REQUEST_MEMORY, classAd);

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
        logger.info("ENTERING export(String dagName, File workDir, Graph<JobNode, JobEdge> graph)");
        return export(dagName, workDir, graph, true);
    }

    public CondorJob export(String dagName, File workDir, Graph<CondorJob, CondorJobEdge> graph,
            boolean includeGlideinRequirements) {
        logger.info("ENTERING export(String dagName, File workDir, Graph<JobNode, JobEdge> graph)");

        CondorJob dagSubmitJob = new CondorJob();
        dagSubmitJob.setName(dagName);
        File dagFile = new File(workDir, dagName + ".dag");
        dagSubmitJob.setSubmitFile(dagFile);

        try {

            if (graph != null && graph.vertexSet().size() > 0) {

                ClassAdvertisement classAd = null;
                for (CondorJob job : graph.vertexSet()) {

                    classAd = ClassAdvertisementFactory.getClassAd(CLASS_AD_KEY_EXECUTABLE).clone();
                    classAd.setValue(job.getExecutable().getPath());
                    job.getClassAdvertismentMap().put(CLASS_AD_KEY_EXECUTABLE, classAd);

                    classAd = ClassAdvertisementFactory.getClassAd(CLASS_AD_KEY_OUTPUT).clone();
                    classAd.setValue(String.format("%s/%s.out", workDir.getAbsolutePath(), job.getName()));
                    job.getClassAdvertismentMap().put(CLASS_AD_KEY_OUTPUT, classAd);

                    classAd = ClassAdvertisementFactory.getClassAd(CLASS_AD_KEY_ERROR).clone();
                    classAd.setValue(String.format("%s/%s.err", workDir.getAbsolutePath(), job.getName()));
                    job.getClassAdvertismentMap().put(CLASS_AD_KEY_ERROR, classAd);

                    classAd = ClassAdvertisementFactory.getClassAd(CLASS_AD_KEY_LOG).clone();
                    classAd.setValue(String.format("%s/%s.log", workDir.getAbsolutePath(), job.getName()));
                    job.getClassAdvertismentMap().put(CLASS_AD_KEY_LOG, classAd);

                    classAd = ClassAdvertisementFactory.getClassAd(CLASS_AD_KEY_REQUEST_CPUS).clone();
                    classAd.setValue(String.format("%d", job.getNumberOfProcessors()));
                    job.getClassAdvertismentMap().put(CLASS_AD_KEY_REQUEST_CPUS, classAd);

                    classAd = ClassAdvertisementFactory.getClassAd(CLASS_AD_KEY_REQUEST_MEMORY).clone();
                    classAd.setValue(String.format("%d", job.getMemory()));
                    job.getClassAdvertismentMap().put(CLASS_AD_KEY_REQUEST_MEMORY, classAd);

                    classAd = ClassAdvertisementFactory.getClassAd(CLASS_AD_KEY_REQUIREMENTS).clone();

                    String requirements = "(Arch == \"X86_64\") && (OpSys == \"LINUX\") && (Memory >= 500) && (Disk >= 0)";

                    if (includeGlideinRequirements) {

                        if (StringUtils.isNotEmpty(job.getSiteName())) {
                            requirements += String.format(" && (TARGET.JLRM_SITE_NAME == \"%s\")", job.getSiteName());
                        }

                        requirements += String.format(
                                " && (TARGET.JLRM_USER == \"%s\") && (TARGET.IS_GLIDEIN == True)",
                                System.getProperty("user.name"));
                    }

                    classAd.setValue(requirements);
                    job.getClassAdvertismentMap().put(CLASS_AD_KEY_REQUIREMENTS, classAd);

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

    private File writeSubmitFile(File submitDir, CondorJob job) throws IOException {
        File submitFile = new File(submitDir, String.format("%s.sub", job.getName()));
        FileWriter submitFileWriter = new FileWriter(submitFile);
        for (ClassAdvertisement classAd : job.getClassAdvertismentMap().values()) {
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