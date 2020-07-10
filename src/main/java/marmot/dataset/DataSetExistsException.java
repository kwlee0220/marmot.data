package marmot.dataset;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class DataSetExistsException extends DataSetException {
	private static final long serialVersionUID = 1L;

	public DataSetExistsException(String name) {
		super(name);
	}

	public DataSetExistsException(String details, Throwable cause) {
		super(details, cause);
	}
}
