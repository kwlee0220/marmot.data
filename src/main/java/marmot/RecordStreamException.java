package marmot;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class RecordStreamException extends RuntimeException {
	private static final long serialVersionUID = 1L;

	public RecordStreamException(String details) {
		super(details);
	}

	public RecordStreamException(String details, Throwable cause) {
		super(details + ", cause=" + cause);
	}
}
