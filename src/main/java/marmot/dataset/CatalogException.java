package marmot.dataset;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class CatalogException extends RuntimeException {
	private static final long serialVersionUID = 1L;

	public CatalogException(String details) {
		super(details);
	}

	public CatalogException(Throwable cause) {
		super(cause);
	}
}
