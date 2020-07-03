package marmot.stream;

import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class CloserAttachedRecordStream extends AbstractRecordStream {
	private final RecordStream m_input;
	private final Runnable m_closer;
	
	public CloserAttachedRecordStream(RecordStream iter, Runnable closer) {
		m_input = iter;
		m_closer = closer;
	}
	
	@Override
	protected void closeInGuard() {
		m_input.close();
		
		synchronized ( this ) {
			m_closer.run();
		}
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_input.getRecordSchema();
	}
	
	@Override
	public boolean next(Record record) {
		return m_input.next(record);
	}
	
	@Override
	public Record nextCopy() {
		return m_input.nextCopy();
	}
}
