package marmot.file;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MarmotFileException extends RuntimeException {
	private static final long serialVersionUID = 1L;

	public MarmotFileException(String details) {
		super(details);
	}

	public MarmotFileException(String details, Throwable cause) {
		super(details + ", cause=" + cause);
	}
}
