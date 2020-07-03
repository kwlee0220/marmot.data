package marmot.stream;

import marmot.Record;
import marmot.RecordSchema;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class FStreamRecordStream extends AbstractRecordStream {
	private final RecordSchema m_schema;
	private final FStream<? extends Record> m_stream;
	
	public FStreamRecordStream(RecordSchema schema, FStream<? extends Record> stream) {
		m_schema = schema;
		m_stream = stream;
	}
	
	@Override
	public RecordSchema getRecordSchema() {
		return m_schema;
	}
	
	@Override
	protected void closeInGuard() throws Exception {
		m_stream.closeQuietly();
	}
	
	@Override
	public Record nextCopy() {
		checkNotClosed();
		
		return m_stream.next().getOrNull();
	}
}