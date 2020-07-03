package marmot;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class DataSetNotFoundException extends DataSetException {
	private static final long serialVersionUID = 1L;
	
	public DataSetNotFoundException(String details) {
		super(details);
	}

	public DataSetNotFoundException(String details, Throwable cause) {
		super(details + ", cause=" + cause);
	}
}
