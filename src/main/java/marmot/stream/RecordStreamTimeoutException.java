package marmot.stream;

import marmot.RecordStreamException;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class RecordStreamTimeoutException extends RecordStreamException {
	private static final long serialVersionUID = 1L;

	public RecordStreamTimeoutException(String details) {
		super(details);
	}
}
