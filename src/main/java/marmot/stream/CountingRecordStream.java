package marmot.stream;

import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordStream;
import utils.Utilities;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class CountingRecordStream extends AbstractRecordStream {
	private final RecordStream m_stream;
	private long m_count = 0;
	
	public CountingRecordStream(RecordStream stream) {
		Utilities.checkNotNullArgument(stream, "input RecordStream is null");
		
		m_stream = stream;
	}

	@Override
	protected void closeInGuard() {
		m_stream.closeQuietly();
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_stream.getRecordSchema();
	}
	
	public long getCount() {
		return m_count;
	}
	
	@Override
	public boolean next(Record output) {
		if ( m_stream.next(output) ) {
			++m_count;
			return true;
		}
		else {
			return false;
		}
	}
	
	@Override
	public Record nextCopy() {
		Record next = m_stream.nextCopy();
		if ( next != null ) {
			++m_count;
		}
		
		return next;
	}
	
	@Override
	public String toString() {
		return m_stream.toString() + "(" + m_count + ")";
	}
}