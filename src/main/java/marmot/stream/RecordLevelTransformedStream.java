package marmot.stream;

import marmot.DefaultRecord;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class RecordLevelTransformedStream extends AbstractRecordStream {
	private final RecordStream m_stream;
	private final Record m_input;
	
	abstract protected boolean transform(Record input, Record output);
	
	protected RecordLevelTransformedStream(RecordStream stream) {
		m_stream = stream;
		m_input = DefaultRecord.of(m_stream.getRecordSchema());
	}
	
	public RecordSchema getInputRecordSchema() {
		return m_stream.getRecordSchema();
	}
	
	@Override
	public boolean next(Record output) {
		while ( m_stream.next(m_input) ) {
			try {
				if ( transform(m_input, output) ) {
					return true;
				}
			}
			catch ( Throwable e ) {
				getLogger().warn("ignored transform failure: op=" + this + ", cause=" + e);
			}
		}
		
		return false;
	}
}
