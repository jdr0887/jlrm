package org.renci.jlrm.condor.cli;

import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.junit.Test;
import org.renci.jlrm.condor.CondorJobStatusType;

public class ParseDAGTest {

    @Test
    public void testParseDAG() {

        boolean allJobsCompleted = false;
        Date date = null;
        int done = 0;
        int pre = 0;
        int queued = 0;
        int post = 0;
        int ready = 0;
        int unReady = 0;
        int failed = 0;
        int held = 0;
        int totalChildrenJobs = 0;

        long startTime = System.currentTimeMillis();
        BufferedReader br = null;
        try {
            br = IOUtils.toBufferedReader(new InputStreamReader(this.getClass().getClassLoader()
                    .getResourceAsStream("org/renci/jlrm/condor/cli/NIDAUCSFSymlink.dag.dagman.out")));
            // br = IOUtils.toBufferedReader(new InputStreamReader(this.getClass().getClassLoader()
            // .getResourceAsStream("org/renci/jlrm/condor/cli/NIDAUCSFVariantCalling.dag.dagman.out")));

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
                        try {
                            date = DateUtils.parseDate(match, new String[] { "MM/dd/yy HH:mm:ss" });
                        } catch (ParseException e) {
                            e.printStackTrace();
                        }

                        match = matcher.group(2);
                        done = Integer.valueOf(match);

                        match = matcher.group(3);
                        pre = Integer.valueOf(match);

                        match = matcher.group(4);
                        queued = Integer.valueOf(match);

                        match = matcher.group(5);
                        post = Integer.valueOf(match);

                        match = matcher.group(6);
                        ready = Integer.valueOf(match);

                        match = matcher.group(7);
                        unReady = Integer.valueOf(match);

                        match = matcher.group(8);
                        failed = Integer.valueOf(match);
                    }

                    String heldLine = br.readLine();
                    pattern = Pattern.compile("^.+ (\\d*) job proc(s) currently held$");
                    matcher = pattern.matcher(heldLine);
                    matches = matcher.matches();
                    if (matches) {
                        String heldCount = matcher.group(1);
                        held = Integer.valueOf(heldCount);
                    }

                    String allJobsCompletedLine = br.readLine();
                    if (allJobsCompletedLine.contains("All jobs Completed")) {
                        allJobsCompleted = true;
                    }

                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                br.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        boolean completed = done == totalChildrenJobs && allJobsCompleted;
        boolean running = queued > 0 && queued < totalChildrenJobs;
        CondorJobStatusType ret = CondorJobStatusType.UNEXPANDED;
        if (completed) {
            ret = CondorJobStatusType.COMPLETED;
        } else if (!completed && running) {
            ret = CondorJobStatusType.RUNNING;
        }
        long endTime = System.currentTimeMillis();
        System.out.println("Duration: " + (endTime - startTime));

        assertTrue(ret == CondorJobStatusType.COMPLETED);
        // assertTrue(ret == CondorJobStatusType.RUNNING);
    }

}
