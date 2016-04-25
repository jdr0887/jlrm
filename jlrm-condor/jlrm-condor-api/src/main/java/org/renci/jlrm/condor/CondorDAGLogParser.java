package org.renci.jlrm.condor;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.renci.jlrm.JLRMException;

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

        List<String> lines = new ArrayList<>();
        // try (RandomAccessFile raf = new RandomAccessFile(logFile, "r")) {
        // long length = raf.length();
        // if (length > 1500) {
        // raf.seek(length - 1500);
        // String line;
        // while ((line = raf.readLine()) != null) {
        // lines.add(line);
        // }
        // }
        // } catch (IOException e) {
        // e.printStackTrace();
        // }

        try {
            lines.addAll(FileUtils.readLines(logFile));
            // Collections.reverse(lines.subList(0, lines.size()));
        } catch (IOException e) {
            e.printStackTrace();
        }

        Matcher matcher = null;

        int totalChildrenJobs = 0;
        for (String line : lines) {
            matcher = dagTotalJobsPattern.matcher(line);
            if (matcher.matches()) {
                String count = matcher.group(1);
                if (StringUtils.isNotEmpty(count)) {
                    totalChildrenJobs = Integer.valueOf(count);
                    break;
                }
            }
        }

        String lastLine = lines.get(lines.size() - 1);

        boolean hasExited = false;
        int exitCode = 0;

        matcher = dagExitStatusPattern.matcher(lastLine);
        if (matcher.matches()) {
            hasExited = true;
            if (matcher.matches()) {
                String code = matcher.group(1);
                if (StringUtils.isNotEmpty(code)) {
                    exitCode = Integer.valueOf(code);
                }
            }
        }

        CondorDAGJobStatusType dagJobStatusType = CondorDAGJobStatusType.OK;
        ListIterator<String> reverseIter = lines.listIterator(lines.size());

        main: while (reverseIter.hasPrevious()) {
            String line = reverseIter.previous();
            matcher = dagStatusPattern.matcher(line);
            if (matcher.matches()) {
                String code = matcher.group(1);
                if (StringUtils.isNotEmpty(code)) {
                    for (CondorDAGJobStatusType type : CondorDAGJobStatusType.values()) {
                        if (type.getCode() == Integer.valueOf(code)) {
                            dagJobStatusType = type;
                            break main;
                        }
                    }
                }
            }
        }

        if (totalChildrenJobs == 0) {
            // dagman with no children???
            ret = CondorJobStatusType.COMPLETED;
            ret.setDagJobStatusType(dagJobStatusType);
            return ret;
        }

        CondorJobTally tally = getLatestTally(lines, totalChildrenJobs);

        if (hasExited) {
            // find termination state

            reverseIter = lines.listIterator(lines.size());

            while (reverseIter.hasPrevious()) {
                String line = reverseIter.previous();

                if (line.contains("All jobs Completed") && tally.getDone().equals(totalChildrenJobs)) {
                    ret = CondorJobStatusType.COMPLETED;
                    break;
                }

                if (line.contains("Running: condor_rm -const DAGManJobId")) {
                    ret = CondorJobStatusType.REMOVED;
                    break;
                }

                if (line.contains("Aborting DAG") && (tally.getFailed() > 0 || dagJobStatusType.equals(CondorDAGJobStatusType.REMOVED))) {
                    ret = CondorJobStatusType.REMOVED;
                    break;
                }
                
            }

        } else {

            if (tally.getQueued() > 0 && tally.getQueued() < totalChildrenJobs) {
                ret = CondorJobStatusType.RUNNING;
            }

        }
        ret.setDagJobStatusType(dagJobStatusType);

        return ret;

    }

    private CondorJobTally getLatestTally(List<String> dagLogOutFileLines, int totalChildrenJobs) {
        CondorJobTally ret = new CondorJobTally();

        ListIterator<String> lineIter = dagLogOutFileLines.listIterator(dagLogOutFileLines.size());
        while (lineIter.hasPrevious()) {
            String line = lineIter.previous();
            if (line.contains(String.format("Of %d nodes total", totalChildrenJobs))) {
                lineIter.next();
                lineIter.next();
                lineIter.next();
                String tallies = lineIter.next();
                Pattern pattern = Pattern.compile(
                        "^(\\d*/\\d*/\\d*\\s.\\d*:\\d*:\\d*)\\s+(\\d*)\\s+(\\d*)\\s+(\\d*)\\s+(\\d*)\\s+(\\d*)\\s+(\\d*)\\s+(\\d*)");
                Matcher matcher = pattern.matcher(tallies);
                boolean matches = matcher.matches();
                if (matches) {

                    String match = matcher.group(1);
                    try {
                        ret.setDate(DateUtils.parseDate(match, new String[] { "MM/dd/yy HH:mm:ss" }));
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }

                    match = matcher.group(2);
                    ret.setDone(Integer.valueOf(match));

                    match = matcher.group(3);
                    ret.setPre(Integer.valueOf(match));

                    match = matcher.group(4);
                    ret.setQueued(Integer.valueOf(match));

                    match = matcher.group(5);
                    ret.setPost(Integer.valueOf(match));

                    match = matcher.group(6);
                    ret.setReady(Integer.valueOf(match));

                    match = matcher.group(7);
                    ret.setUnReady(Integer.valueOf(match));

                    match = matcher.group(8);
                    ret.setFailed(Integer.valueOf(match));

                }

                String heldLine = lineIter.next();
                pattern = Pattern.compile("^.+ (\\d*) job proc(s) currently held$");
                matcher = pattern.matcher(heldLine);
                matches = matcher.matches();
                if (matches) {
                    String heldCount = matcher.group(1);
                    ret.setHeld(Integer.valueOf(heldCount));
                }
                break;
            }
        }
        return ret;
    }

}
