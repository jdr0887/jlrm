package org.renci.jlrm.sge.ssh;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Test;

public class Scratch {

    @Test
    public void testSubmitOutputRegex() {
        String line = "Your job 609505 (\"Test\") has been submitted";
        Pattern pattern = Pattern.compile("^.+job (\\d+) .+has been submitted$");
        Matcher matcher = pattern.matcher(line);
        assert (matcher.matches());
        matcher.find();
        System.out.println(matcher.group(1));
    }
}
