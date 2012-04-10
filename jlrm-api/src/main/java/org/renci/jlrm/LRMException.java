package org.renci.jlrm;

public class LRMException extends Exception {

	private static final long serialVersionUID = 4615997972669079151L;

	public LRMException() {
		super();
	}

	public LRMException(String message, Throwable cause) {
		super(message, cause);
	}

	public LRMException(String message) {
		super(message);
	}

	public LRMException(Throwable cause) {
		super(cause);
	}

}
