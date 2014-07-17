package org.renci.jlrm.condor;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.List;
import java.util.ListIterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.renci.jlrm.JLRMException;

public class CondorDAGLogParser {

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

        boolean hasExited = false;
        int totalChildrenJobs = 0;

        if (!logFile.exists()) {
            ret = CondorJobStatusType.IDLE;
            return ret;
        }

        try {
            List<String> lines = FileUtils.readLines(logFile);
            String lastLine = lines.get(lines.size() - 1);

            if (lastLine.contains("EXITING WITH STATUS")) {
                // we now know what the dag is finished...just not how
                hasExited = true;
            }

            ListIterator<String> lineIter = lines.listIterator();
            while (lineIter.hasNext()) {
                String line = lineIter.next();
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

            if (totalChildrenJobs == 0) {
                // dagman with no children???
                return CondorJobStatusType.COMPLETED;
            }

            CondorJobTally tally = getLatestTally(lines, totalChildrenJobs);

            if (hasExited) {
                // find termination state

                lineIter = lines.listIterator(lines.size());
                while (lineIter.hasPrevious()) {
                    String line = lineIter.previous();
                    if (line.contains("All jobs Completed") && tally.getDone().equals(totalChildrenJobs)) {
                        ret = CondorJobStatusType.COMPLETED;
                        break;
                    }
                    if (line.contains("Running: condor_rm -const DAGManJobId")) {
                        ret = CondorJobStatusType.REMOVED;
                        break;
                    }
                    if (line.contains("Aborting DAG") && tally.getFailed() > 0) {
                        ret = CondorJobStatusType.REMOVED;
                        break;
                    }
                }

            } else {

                if (tally.getQueued() > 0 && tally.getQueued() < totalChildrenJobs) {
                    ret = CondorJobStatusType.RUNNING;
                }

            }
        } catch (IOException e1) {
            e1.printStackTrace();
        }

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
                Pattern pattern = Pattern
                        .compile("^(\\d*/\\d*/\\d*\\s.\\d*:\\d*:\\d*)\\s+(\\d*)\\s+(\\d*)\\s+(\\d*)\\s+(\\d*)\\s+(\\d*)\\s+(\\d*)\\s+(\\d*)");
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
