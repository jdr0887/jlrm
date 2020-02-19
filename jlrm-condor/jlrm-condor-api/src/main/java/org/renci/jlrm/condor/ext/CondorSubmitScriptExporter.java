package org.renci.jlrm.condor.ext;

import static org.renci.jlrm.condor.ClassAdvertisementFactory.CLASS_AD_KEY_ERROR;
import static org.renci.jlrm.condor.ClassAdvertisementFactory.CLASS_AD_KEY_EXECUTABLE;
import static org.renci.jlrm.condor.ClassAdvertisementFactory.CLASS_AD_KEY_LOG;
import static org.renci.jlrm.condor.ClassAdvertisementFactory.CLASS_AD_KEY_OUTPUT;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.lang3.StringUtils;
import org.jgrapht.Graph;
import org.renci.jlrm.condor.ClassAdvertisement;
import org.renci.jlrm.condor.ClassAdvertisementFactory;
import org.renci.jlrm.condor.CondorJob;
import org.renci.jlrm.condor.CondorJobEdge;

import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@NoArgsConstructor
@Slf4j
public class CondorSubmitScriptExporter {

    public CondorJob export(Path workDir, CondorJob job) {
        try {

            ClassAdvertisement classAd = ClassAdvertisementFactory.getClassAd(CLASS_AD_KEY_EXECUTABLE).clone();
            classAd.setValue(job.getExecutable().toAbsolutePath().toString());
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

            Path submitFile = writeSubmitFile(workDir, job);
            job.setSubmitFile(submitFile);
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return job;
    }

    public CondorJob export(String dagName, Path workDir, Graph<CondorJob, CondorJobEdge> graph) {
        log.debug("ENTERING export(String dagName, File workDir, Graph<JobNode, JobEdge> graph)");
        return export(dagName, workDir, graph, true);
    }

    public CondorJob export(String dagName, Path workDir, Graph<CondorJob, CondorJobEdge> graph,
            boolean includeGlideinRequirements) {
        log.debug("ENTERING export(String dagName, File workDir, Graph<JobNode, JobEdge> graph)");

        Path dagFile = Paths.get(workDir.toAbsolutePath().toString(), dagName + ".dag");

        CondorJob dagSubmitJob = CondorJob.builder().name(dagName).submitFile(dagFile).build();

        try {

            if (graph != null && graph.vertexSet().size() > 0) {

                ClassAdvertisement classAd = null;
                for (CondorJob job : graph.vertexSet()) {

                    classAd = ClassAdvertisementFactory.getClassAd(CLASS_AD_KEY_EXECUTABLE).clone();
                    classAd.setValue(job.getExecutable().toAbsolutePath().toString());
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
                        job.addRequirement(
                                String.format("TARGET.JLRM_USER == \"%s\"", System.getProperty("user.name")));
                        job.addRequirement("TARGET.IS_GLIDEIN == True");
                    }

                }

                try (BufferedWriter bw = Files.newBufferedWriter(dagFile)) {

                    for (CondorJob job : graph.vertexSet()) {
                        writeSubmitFile(workDir, job);
                        bw.write(String.format("%n%1$-10s %2$-10s %2$s.sub", "JOB", job.getName()));
                        if (StringUtils.isNotEmpty(job.getPreScript())) {
                            bw.write(String.format("%n%1$-10s %2$-10s %3$-10s %4$-10s", "SCRIPT", "PRE", job.getName(),
                                    job.getPreScript()));
                        }
                        if (StringUtils.isNotEmpty(job.getPostScript())) {
                            bw.write(String.format("%n%1$-10s %2$-10s %3$-10s %4$-10s", "SCRIPT", "POST", job.getName(),
                                    job.getPostScript()));
                        }
                        if (job.getRetry() != null && job.getRetry() > 1) {
                            bw.write(String.format("%n%1$-10s %2$-10s %3$d%n", "RETRY", job.getName(), job.getRetry()));
                        }
                        bw.flush();
                    }

                    bw.write(System.getProperty("line.separator"));

                    for (CondorJobEdge edge : graph.edgeSet()) {
                        CondorJob source = (CondorJob) edge.getSource();
                        CondorJob target = (CondorJob) edge.getTarget();
                        String format = "%1$-10s %2$-10s %3$-10s %4$s%n";
                        bw.write(String.format(format, "PARENT", source.getName(), "CHILD", target.getName()));
                        bw.flush();
                    }

                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }

            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

        return dagSubmitJob;
    }

    protected Path writeSubmitFile(Path submitDir, CondorJob job) throws IOException {
        Path submitFile = Paths.get(submitDir.toAbsolutePath().toString(), String.format("%s.sub", job.getName()));
        try (BufferedWriter bw = Files.newBufferedWriter(submitFile)) {

            for (ClassAdvertisement classAd : job.getClassAdvertisments()) {
                switch (classAd.getType()) {
                    case BOOLEAN:
                    case EXPRESSION:
                    case INTEGER:
                        bw.write(String.format("%1$-25s = %2$s%n", classAd.getKey(), classAd.getValue()));
                        break;
                    case STRING:
                    default:
                        bw.write(String.format("%1$-25s = \"%2$s\"%n", classAd.getKey(), classAd.getValue()));
                        break;
                }
                bw.flush();
            }
            bw.write(String.format("%s%n",
                    ClassAdvertisementFactory.getClassAd(ClassAdvertisementFactory.CLASS_AD_KEY_QUEUE).getKey()));
            bw.flush();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return submitFile;
    }

}