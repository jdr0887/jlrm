package org.renci.jlrm.condor;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.input.ReversedLinesFileReader;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.renci.jlrm.JLRMException;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CondorDAGLogParser {

    private static final Pattern dagStatusPattern = Pattern.compile("^.+DAG status: (\\d*).+$");

    // private static final Pattern dagTotalJobsPattern = Pattern.compile("^.+Of (\\d*) nodes total:$");
    private static final Pattern dagTotalJobsPattern = Pattern.compile("^.+Dag contains (\\d*) total jobs$");

    private static final Pattern dagExitStatusPattern = Pattern.compile("^.+EXITING WITH STATUS (\\d*)$");

    private static CondorDAGLogParser instance = null;

    public static CondorDAGLogParser getInstance() {
        if (instance == null) {
            instance = new CondorDAGLogParser();
        }
        return instance;
    }

    private CondorDAGLogParser() {
        super();
    }

    public CondorJobStatusType parse(File logFile) throws JLRMException {

        if (logFile == null) {
            throw new JLRMException("logFile can't be null");
        }

        CondorJobStatusType ret = CondorJobStatusType.UNEXPANDED;

        if (!logFile.exists()) {
            ret = CondorJobStatusType.IDLE;
            return ret;
        }

        int totalChildrenJobs = 0;
        try (FileReader fr = new FileReader(logFile); BufferedReader br = new BufferedReader(fr)) {
            String line;
            while ((line = br.readLine()) != null) {
                Matcher matcher = dagTotalJobsPattern.matcher(line);
                if (matcher.matches()) {
                    String count = matcher.group(1);
                    if (StringUtils.isNotEmpty(count)) {
                        totalChildrenJobs = Integer.valueOf(count);
                        break;
                    }
                }
            }
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }

        boolean hasExited = false;

        int exitCode = 0;
        try (ReversedLinesFileReader fr = new ReversedLinesFileReader(logFile, StandardCharsets.UTF_8)) {
            Matcher matcher = dagExitStatusPattern.matcher(fr.readLine());
            if (matcher.matches()) {
                hasExited = true;
                String code = matcher.group(1);
                if (StringUtils.isNotEmpty(code)) {
                    exitCode = Integer.valueOf(code);
                }
            }
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }

        CondorDAGJobStatusType dagJobStatusType = CondorDAGJobStatusType.OK;
        try (ReversedLinesFileReader fr = new ReversedLinesFileReader(logFile, StandardCharsets.UTF_8)) {
            String line;
            while ((line = fr.readLine()) != null) {
                Matcher matcher = dagStatusPattern.matcher(line);
                if (matcher.matches()) {
                    String code = matcher.group(1);
                    if (StringUtils.isNotEmpty(code)) {
                        CondorDAGJobStatusType type = Arrays.asList(CondorDAGJobStatusType.values()).stream()
                                .filter(a -> a.getCode().equals(Integer.valueOf(code))).findFirst().orElse(null);

                        if (type != null) {
                            dagJobStatusType = type;
                        }
                        break;

                    }
                }
            }
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }

        if (totalChildrenJobs == 0) {
            // dagman with no children???
            ret = CondorJobStatusType.COMPLETED;
            ret.setDagJobStatusType(dagJobStatusType);
            return ret;
        }

        CondorJobTally tally = new CondorJobTally();

        try (ReversedLinesFileReader fr = new ReversedLinesFileReader(logFile, StandardCharsets.UTF_8)) {
            String line;
            while ((line = fr.readLine()) != null) {

                Pattern pattern = Pattern.compile("^.+ (\\d*) job proc\\(s\\) currently held$");
                Matcher matcher = pattern.matcher(line);
                if (!matcher.matches()) {
                    continue;
                }

                String heldCount = matcher.group(1);
                tally.setHeld(Integer.valueOf(heldCount));

                String tallies = fr.readLine();
                pattern = Pattern.compile(
                        "^(\\d*/\\d*/\\d*\\s.\\d*:\\d*:\\d*)\\s+(\\d*)\\s+(\\d*)\\s+(\\d*)\\s+(\\d*)\\s+(\\d*)\\s+(\\d*)\\s+(\\d*)");
                matcher = pattern.matcher(tallies);
                if (matcher.matches()) {

                    String match = matcher.group(1);
                    try {
                        tally.setDate(DateUtils.parseDate(match, new String[] { "MM/dd/yy HH:mm:ss" }));
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }

                    match = matcher.group(2);
                    tally.setDone(Integer.valueOf(match));

                    match = matcher.group(3);
                    tally.setPre(Integer.valueOf(match));

                    match = matcher.group(4);
                    tally.setQueued(Integer.valueOf(match));

                    match = matcher.group(5);
                    tally.setPost(Integer.valueOf(match));

                    match = matcher.group(6);
                    tally.setReady(Integer.valueOf(match));

                    match = matcher.group(7);
                    tally.setUnReady(Integer.valueOf(match));

                    match = matcher.group(8);
                    tally.setFailed(Integer.valueOf(match));

                }

                break;
            }

        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }

        if (hasExited) {
            // find termination state

            try (ReversedLinesFileReader fr = new ReversedLinesFileReader(logFile, StandardCharsets.UTF_8)) {
                String line;
                while ((line = fr.readLine()) != null) {
                    if (line.contains("All jobs Completed") && tally.getDone().equals(totalChildrenJobs)) {
                        ret = CondorJobStatusType.COMPLETED;
                        break;
                    }

                    if (line.contains("Running: condor_rm -const DAGManJobId")) {
                        ret = CondorJobStatusType.REMOVED;
                        break;
                    }

                    if (line.contains("Aborting DAG")
                            && (tally.getFailed() > 0 || dagJobStatusType.equals(CondorDAGJobStatusType.REMOVED))) {
                        ret = CondorJobStatusType.REMOVED;
                        break;
                    }
                }
            } catch (IOException e) {
                log.error(e.getMessage(), e);
            }

        } else {

            if (tally.getQueued() > 0 && tally.getQueued() < totalChildrenJobs) {
                ret = CondorJobStatusType.RUNNING;
            }

        }
        ret.setDagJobStatusType(dagJobStatusType);

        return ret;

    }

}
