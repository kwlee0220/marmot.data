package marmot.optor;

import marmot.Record;
import marmot.RecordReader;
import marmot.RecordSchema;
import marmot.RecordStream;
import marmot.stream.AbstractRecordStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TakenReader implements RecordReader {
	private final RecordReader m_input;
	private long m_count;
	
	public TakenReader(RecordReader input, long count) {
		m_input = input;
		m_count = count;
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_input.getRecordSchema();
	}

	@Override
	public RecordStream read() {
		return new ResultStream(m_input.read(), m_count);
	}

	public static class ResultStream extends AbstractRecordStream {
		private final RecordStream m_stream;
		private long m_remains;
		
		public ResultStream(RecordStream stream, long count) {
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
		public Record next() {
			checkNotClosed();
	
			if ( m_remains <= 0 ) {
				return null;
			}
			else {
				--m_remains;
				return m_stream.next();
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

}
