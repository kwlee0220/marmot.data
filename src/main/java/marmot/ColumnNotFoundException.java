package marmot;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ColumnNotFoundException extends RuntimeException {
	private static final long serialVersionUID = 1L;

	public ColumnNotFoundException(String name) {
		super(name);
	}
}
