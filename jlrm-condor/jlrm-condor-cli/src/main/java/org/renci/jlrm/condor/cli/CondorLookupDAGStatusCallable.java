package org.renci.jlrm.condor.cli;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.util.Date;
import java.util.concurrent.Callable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.renci.jlrm.JLRMException;
import org.renci.jlrm.condor.CondorJob;
import org.renci.jlrm.condor.CondorJobStatusType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CondorLookupDAGStatusCallable implements Callable<CondorJobStatusType> {

    private final Logger logger = LoggerFactory.getLogger(CondorLookupDAGStatusCallable.class);

    private CondorJob job;

    public CondorLookupDAGStatusCallable(CondorJob job) {
        super();
        this.job = job;
    }

    @Override
    public CondorJobStatusType call() throws JLRMException {
        logger.info("ENTERING call()");
        CondorJobStatusType ret = CondorJobStatusType.UNEXPANDED;

        boolean allJobsCompleted = false;
        int totalChildrenJobs = 0;
        Date date = null;
        int done = 0;
        int pre = 0;
        int queued = 0;
        int post = 0;
        int ready = 0;
        int unReady = 0;
        int failed = 0;
        int held = 0;

        File dagmanOutFile = new File(job.getSubmitFile().getParentFile(), job.getSubmitFile().getName()
                .replace(".dag", ".dag.dagman.out"));

        if (!dagmanOutFile.exists()) {
            ret = CondorJobStatusType.IDLE;
            return ret;
        }

        BufferedReader br = null;
        try {

            br = new BufferedReader(new FileReader(dagmanOutFile));

            String line;
            while ((line = br.readLine()) != null) {
                if (line.contains("Dag contains")) {
                    Pattern pattern = Pattern.compile("^.+ Dag contains (\\d*) total jobs$");
                    Matcher matcher = pattern.matcher(line);
                    boolean matches = matcher.matches();
                    if (matches) {
                        String count = matcher.group(1);
                        totalChildrenJobs = Integer.valueOf(count);
                        break;
                    }
                }
            }

            while ((line = br.readLine()) != null) {
                if (line.contains(String.format("Of %d nodes total", totalChildrenJobs))) {
                    br.readLine();
                    br.readLine();
                    String tallies = br.readLine();
                    Pattern pattern = Pattern
                            .compile("^(\\d*/\\d*/\\d*\\s.\\d*:\\d*:\\d*)\\s+(\\d*)\\s+(\\d*)\\s+(\\d*)\\s+(\\d*)\\s+(\\d*)\\s+(\\d*)\\s+(\\d*)");
                    Matcher matcher = pattern.matcher(tallies);
                    boolean matches = matcher.matches();
                    if (matches) {

                        String match = matcher.group(1);
                        if (StringUtils.isNotEmpty(match)) {
                            try {
                                date = DateUtils.parseDate(match, new String[] { "MM/dd/yy HH:mm:ss" });
                            } catch (ParseException e) {
                                e.printStackTrace();
                            }
                        }

                        match = matcher.group(2);
                        if (StringUtils.isNotEmpty(match)) {
                            done = Integer.valueOf(match);
                        }

                        match = matcher.group(3);
                        if (StringUtils.isNotEmpty(match)) {
                            pre = Integer.valueOf(match);
                        }

                        match = matcher.group(4);
                        if (StringUtils.isNotEmpty(match)) {
                            queued = Integer.valueOf(match);
                        }

                        match = matcher.group(5);
                        if (StringUtils.isNotEmpty(match)) {
                            post = Integer.valueOf(match);
                        }

                        match = matcher.group(6);
                        if (StringUtils.isNotEmpty(match)) {
                            ready = Integer.valueOf(match);
                        }

                        match = matcher.group(7);
                        if (StringUtils.isNotEmpty(match)) {
                            unReady = Integer.valueOf(match);
                        }

                        match = matcher.group(8);
                        if (StringUtils.isNotEmpty(match)) {
                            failed = Integer.valueOf(match);
                        }
                    }

                    String heldLine = br.readLine();
                    pattern = Pattern.compile("^.+ (\\d*) job proc(s) currently held$");
                    matcher = pattern.matcher(heldLine);
                    matches = matcher.matches();
                    if (matches) {
                        String heldCount = matcher.group(1);
                        if (StringUtils.isNotEmpty(heldCount)) {
                            held = Integer.valueOf(heldCount);
                        }
                    }

                    String allJobsCompletedLine = br.readLine();
                    if (StringUtils.isNotEmpty(allJobsCompletedLine)
                            && allJobsCompletedLine.contains("All jobs Completed")) {
                        allJobsCompleted = true;
                    }

                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        boolean completed = done == totalChildrenJobs && allJobsCompleted;
        boolean running = queued > 0 && queued < totalChildrenJobs;
        if (completed) {
            ret = CondorJobStatusType.COMPLETED;
        } else if (!completed && running) {
            ret = CondorJobStatusType.RUNNING;
        }

        return ret;
    }

    public CondorJob getJob() {
        return job;
    }

    public void setJob(CondorJob job) {
        this.job = job;
    }

}
