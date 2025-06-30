package marmot.stream;

import javax.annotation.Nullable;

import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordStream;
import utils.Utilities;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PeekableRecordStream extends AbstractRecordStream {
	private final RecordStream m_input;
	private @Nullable Record m_peeked = null;
	
	public PeekableRecordStream(RecordStream input) {
		Utilities.checkNotNullArgument(input, "Peeking RecordSet is null");
		
		m_input = input;
	}
	
	@Override
	protected void closeInGuard() {
		m_input.close();
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_input.getRecordSchema();
	}
	
	public FOption<Record> peek() {
		checkNotClosed();

		if ( m_peeked == null ) {
			m_peeked = m_input.nextCopy();
		}
		
		return FOption.ofNullable(m_peeked);
	}

	@Override
	public Record next() {
		checkNotClosed();
		
		if ( m_peeked != null ) {
			Record peeked = m_peeked;
			m_peeked = null;
			
			return peeked;
		}
		else {
			return m_input.next();
		}
	}
	
	@Override
	public Record nextCopy() {
		checkNotClosed();
		
		if ( m_peeked != null ) {
			Record output = m_peeked;
			m_peeked = null;
			
			return output;
		}
		else {
			return m_input.nextCopy();
		}
	}
}
