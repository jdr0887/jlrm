package org.renci.jlrm;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

public class PerThreadDateFormatter {

    private static final ThreadLocal<SimpleDateFormat> dateFormatHolder = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat("yyyy-MM-dd");
        }
    };

    public static DateFormat getDateFormatter() {
        return dateFormatHolder.get();
    }
}
