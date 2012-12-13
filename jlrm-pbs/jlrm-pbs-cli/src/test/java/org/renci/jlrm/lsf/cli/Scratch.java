package org.renci.jlrm.lsf.cli;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Test;

public class Scratch {

    @Test
    public void testSubmitOutputRegex() {
        String line = "Job <322829> is submitted to queue <week>.";

        // Pattern pattern = Pattern.compile("Job <(\\d*)> is submitted to queue <.+>\\.");
        Pattern pattern = Pattern.compile("^Job.+<(\\d*)> is submitted.+\\.$");
        Matcher matcher = pattern.matcher(line);
        assert (matcher.matches());
        matcher.find();
        System.out.println(matcher.group(1));

    }
}
