package org.renci.jlrm;

public class JLRMException extends Exception {

	private static final long serialVersionUID = 4615997972669079151L;

	public JLRMException() {
		super();
	}

	public JLRMException(String message, Throwable cause) {
		super(message, cause);
	}

	public JLRMException(String message) {
		super(message);
	}

	public JLRMException(Throwable cause) {
		super(cause);
	}

}
