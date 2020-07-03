package marmot.stream;

import java.util.Iterator;

import marmot.Record;
import marmot.RecordSchema;
import utils.io.IOUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class IteratorRecordStream extends AbstractRecordStream {
	private final RecordSchema m_schema;
	private final Iterator<? extends Record> m_iter;
	
	public IteratorRecordStream(RecordSchema schema, Iterator<? extends Record> iter) {
		m_schema = schema;
		m_iter = iter;
	}
	
	@Override
	protected void closeInGuard() {
		IOUtils.closeQuietly(m_iter);
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_schema;
	}
	
	@Override
	public Record nextCopy() {
		checkNotClosed();
		
		return (m_iter.hasNext()) ? m_iter.next() : null;
	}
}