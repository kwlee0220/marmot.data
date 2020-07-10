package marmot.stream;

import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class AutoClosingRecordStream extends AbstractRecordStream {
	private final RecordStream m_stream;
	
	public AutoClosingRecordStream(RecordStream stream) {
		m_stream = stream;
	}
	
	@Override
	public void closeInGuard() {
		m_stream.close();
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_stream.getRecordSchema();
	}
	
	@Override
	public Record next() {
		Record record;
		if ( (record = m_stream.next()) == null ) {
			closeQuietly();
		}
		
		return record;
	}
	
	@Override
	public Record nextCopy() {
		Record next = m_stream.nextCopy();
		if ( next == null ) {
			closeQuietly();
			return null;
		}
		else {
			return next;
		}
	}
}