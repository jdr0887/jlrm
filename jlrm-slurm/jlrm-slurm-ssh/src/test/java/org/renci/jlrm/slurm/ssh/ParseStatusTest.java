package org.renci.jlrm.slurm.ssh;

import java.io.IOException;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import org.renci.jlrm.JobStatusInfo;
import org.renci.jlrm.slurm.SLURMJobStatusType;

public class ParseStatusTest {

    @Test
    public void test() {

        try {
            List<String> lines = IOUtils.readLines(this.getClass().getResourceAsStream("status.txt"));

            lines.forEach(a -> {

                SLURMJobStatusType statusType = SLURMJobStatusType.COMPLETED;
                if (StringUtils.isNotEmpty(a)) {
                    String[] lineSplit = StringUtils.split(a, '|');
                    if (lineSplit != null && lineSplit.length == 4) {
                        for (SLURMJobStatusType type : SLURMJobStatusType.values()) {
                            if (StringUtils.isNotEmpty(lineSplit[1]) && lineSplit[1].contains(type.toString())) {
                                statusType = type;
                                break;
                            }
                        }
                        JobStatusInfo info = new JobStatusInfo(lineSplit[0], statusType.toString(), lineSplit[2],
                                lineSplit[3]);
                        System.out.println(info.toString());
                    }
                }

            });

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
