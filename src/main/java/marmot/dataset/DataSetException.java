package marmot.dataset;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class DataSetException extends RuntimeException {
	private static final long serialVersionUID = 1L;

	public DataSetException(String details) {
		super(details);
	}

	public DataSetException(String details, Throwable cause) {
		super(details + ", cause=" + cause);
	}
}
