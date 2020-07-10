package marmot.optor;

import java.util.function.Predicate;

import marmot.Record;
import marmot.RecordReader;
import marmot.RecordSchema;
import marmot.RecordStream;
import marmot.stream.AbstractRecordStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class FilteredDataSet implements RecordReader {
	private final RecordReader m_input;
	private final Predicate<? super Record> m_pred;
	
	public FilteredDataSet(RecordReader input, Predicate<? super Record> pred) {
		m_input = input;
		m_pred = pred;
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_input.getRecordSchema();
	}

	@Override
	public RecordStream read() {
		return new StreamImpl(m_input.read(), m_pred);
	}

	static class StreamImpl extends AbstractRecordStream {
		private final RecordStream m_input;
		private final Predicate<? super Record> m_pred;
		
		StreamImpl(RecordStream input, Predicate<? super Record> pred) {
			m_input = input;
			m_pred = pred;
		}
		
		@Override
		public RecordSchema getRecordSchema() {
			return m_input.getRecordSchema();
		}

		@Override
		protected void closeInGuard() throws Exception {
			m_input.close();
		}
		
		@Override
		public Record next() {
			Record record;
			while ( (record = m_input.next()) != null ) {
				if ( m_pred.test(record) ) {
					return record;
				}
			}
			
			return null;
		}
		
		@Override
		public Record nextCopy() {
			Record rec;
			while ( (rec = m_input.nextCopy()) != null ) {
				if ( m_pred.test(rec) ) {
					return rec;
				}
			}
			
			return null;
		}
	}
}
