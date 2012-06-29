package org.renci.jlrm.sge;

public enum SGEJobStatusType {

	PENDING("PEND"),

	SUSPENDED_FROM_PENDING("PSUSP"),

	RUNNING("RUN"),

	SUSPENDED_BY_USER("USUSP"),

	SUSPENDED_BY_SYSTEM("SSUSP"),

	DONE("DONE"),

	EXIT("EXIT"),

	UNKNOWN("UNKWN"),

	ZOMBIE("ZOMBI");

	private String value;

	private SGEJobStatusType(String value) {
		this.value = value;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

}
