package org.renci.jlrm.pbs.ssh;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.LineNumberReader;
import java.io.StringReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.junit.Test;
import org.renci.jlrm.pbs.PBSJobStatusInfo;
import org.renci.jlrm.pbs.PBSJobStatusType;

public class Scratch {

    @Test
    public void testPattern() {
        Pattern pattern = Pattern.compile("^(\\d+)\\..+");
        Matcher matcher = pattern.matcher("2805904.brsn.renci.org");
        assertTrue(matcher.matches());
        assertTrue("2805904".equals(matcher.group(1)));
    }

    @Test
    public void parseLookupStatusCommand() {
        try {
            String output = IOUtils.toString(this.getClass().getClassLoader()
                    .getResourceAsStream("org/renci/jlrm/pbs/ssh/lookupStatus.txt"));

            LineNumberReader lnr = new LineNumberReader(new StringReader(output));
            boolean canRead = false;
            String line;
            while ((line = lnr.readLine()) != null) {

                if (StringUtils.isNotEmpty(line)) {

                    if (line.startsWith("---")) {
                        canRead = true;
                        continue;
                    }

                    if (canRead) {
                        PBSJobStatusType statusType = PBSJobStatusType.ENDING;
                        String[] lineSplit = line.split(" ");
                        if (lineSplit != null && lineSplit.length == 4) {
                            for (PBSJobStatusType type : PBSJobStatusType.values()) {
                                if (type.getValue().equals(lineSplit[1])) {
                                    statusType = type;
                                }
                            }
                            String jobId = lineSplit[0].substring(0, lineSplit[0].indexOf("."));
                            PBSJobStatusInfo info = new PBSJobStatusInfo(jobId, statusType, lineSplit[2], lineSplit[3]);
                            System.out.println(info.toString());
                        }
                    }
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
