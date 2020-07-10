package marmot.optor;

import java.util.function.Consumer;

import marmot.Record;
import marmot.RecordReader;
import marmot.RecordSchema;
import marmot.RecordStream;
import marmot.stream.AbstractRecordStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PeekingDataSet implements RecordReader {
	private final RecordReader m_input;
	private final Consumer<Record> m_action;
	
	public PeekingDataSet(RecordReader input, Consumer<Record> action) {
		m_input = input;
		m_action = action;
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_input.getRecordSchema();
	}

	@Override
	public RecordStream read() {
		return new StreamImpl(m_input.read());
	}

	private class StreamImpl extends AbstractRecordStream {
		private final RecordStream m_input;
		
		StreamImpl(RecordStream input) {
			m_input = input;
		}

		@Override
		protected void closeInGuard() throws Exception {
			m_input.close();
		}
		
		@Override
		public RecordSchema getRecordSchema() {
			return m_input.getRecordSchema();
		}
		
		@Override
		public Record next() {
			Record record;
			if ( (record = m_input.next()) != null ) {
				m_action.accept(record);
			}
			
			return record;
		}
		
		@Override
		public Record nextCopy() {
			Record record = m_input.nextCopy();
			if ( record != null ) {
				m_action.accept(record);
			}
			
			return record;
		}
	}
}
