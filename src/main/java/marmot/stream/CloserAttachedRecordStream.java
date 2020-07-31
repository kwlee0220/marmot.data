package marmot.stream;

import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordStream;
import utils.func.CheckedRunnable;
import utils.func.Try;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class CloserAttachedRecordStream extends AbstractRecordStream {
	private final RecordStream m_input;
	private final CheckedRunnable m_closer;
	
	public CloserAttachedRecordStream(RecordStream iter, CheckedRunnable closer) {
		m_input = iter;
		m_closer = closer;
	}
	
	@Override
	protected void closeInGuard() {
		m_input.close();
		
		synchronized ( this ) {
			Try.run(m_closer::run);
		}
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_input.getRecordSchema();
	}
	
	@Override
	public Record next() {
		return m_input.next();
	}
	
	@Override
	public Record nextCopy() {
		return m_input.nextCopy();
	}
}
