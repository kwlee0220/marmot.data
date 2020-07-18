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
	private Record m_output;
	
	abstract protected boolean transform(Record input, Record output);
	
	protected RecordLevelTransformedStream(RecordStream stream) {
		m_stream = stream;
	}

	@Override
	protected void closeInGuard() throws Exception {
		m_stream.close();
	}
	
	public RecordSchema getInputRecordSchema() {
		return m_stream.getRecordSchema();
	}
	
	@Override
	public Record next() {
		if ( m_output == null ) {
			m_output = DefaultRecord.of(getRecordSchema());
		}
		
		Record record;
		while ( (record = m_stream.next()) != null ) {
			try {
				if ( transform(record, m_output) ) {
					return m_output;
				}
			}
			catch ( Throwable e ) {
				getLogger().warn("ignored transform failure: op=" + this + ", cause=" + e);
			}
		}
		
		return null;
	}
}
