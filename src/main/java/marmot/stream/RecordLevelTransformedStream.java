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
	private final Record m_output;
	
	abstract protected boolean transform(Record input, Record output);
	
	protected RecordLevelTransformedStream(RecordStream stream) {
		m_stream = stream;
		m_output = DefaultRecord.of(stream.getRecordSchema());
	}
	
	public RecordSchema getInputRecordSchema() {
		return m_stream.getRecordSchema();
	}
	
	@Override
	public Record next() {
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
