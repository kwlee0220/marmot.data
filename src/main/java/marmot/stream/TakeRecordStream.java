package marmot.stream;

import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TakeRecordStream extends AbstractRecordStream {
	private final RecordStream m_stream;
	private long m_remains;
	
	public TakeRecordStream(RecordStream stream, long count) {
		m_stream = stream;
		m_remains = count;
	}

	@Override
	protected void closeInGuard() throws Exception {
		m_stream.close();
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_stream.getRecordSchema();
	}

	@Override
	public boolean next(Record output) {
		checkNotClosed();

		if ( m_remains <= 0 ) {
			return false;
		}
		else {
			--m_remains;
			return m_stream.next(output);
		}
	}
	
	@Override
	public Record nextCopy() {
		checkNotClosed();

		if ( m_remains <= 0 ) {
			return null;
		}
		else {
			--m_remains;
			return m_stream.nextCopy();
		}
	}
}
