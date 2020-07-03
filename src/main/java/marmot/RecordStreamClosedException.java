package marmot;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class RecordStreamClosedException extends RecordStreamException {
	private static final long serialVersionUID = 1L;

	public RecordStreamClosedException(String details) {
		super(details);
	}
}
