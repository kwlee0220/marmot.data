package marmot.file;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MarmotFileNotFoundException extends MarmotFileException {
	private static final long serialVersionUID = 1L;
	
	public MarmotFileNotFoundException(String details) {
		super(details);
	}

	public MarmotFileNotFoundException(String details, Throwable cause) {
		super(details + ", cause=" + cause);
	}
}
