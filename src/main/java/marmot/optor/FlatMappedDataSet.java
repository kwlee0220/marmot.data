package marmot.optor;

import java.util.function.Function;

import marmot.Record;
import marmot.RecordReader;
import marmot.RecordSchema;
import marmot.RecordStream;
import marmot.stream.AbstractRecordStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class FlatMappedDataSet implements RecordReader {
	private final RecordReader m_srcDataSet;
	private final RecordSchema m_outSchema;
	private final Function<? super Record,RecordStream> m_transform;
	
	public FlatMappedDataSet(RecordReader input, RecordSchema outSchema,
							Function<? super Record,RecordStream> transform) {
		m_srcDataSet = input;
		m_transform = transform;
		m_outSchema = outSchema;
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_outSchema;
	}

	@Override
	public RecordStream read() {
		return new StreamImpl(m_srcDataSet.read());
	}
	
	private class StreamImpl extends AbstractRecordStream {
		private final RecordStream m_srcStream;
		
		private Record m_inputRecord;
		private RecordStream m_transformeds;
		
		StreamImpl(RecordStream input) {
			m_srcStream = input;
			
			m_transformeds = RecordStream.empty(m_outSchema);
		}

		@Override
		protected void closeInGuard() throws Exception {
			m_srcStream.close();
		}

		@Override
		public RecordSchema getRecordSchema() {
			return m_outSchema;
		}
		
		@Override
		public Record next() {
			Record record;
			while ( true ) {
				if ( (record = m_transformeds.next()) != null ) {
					return record;
				}
				
				if ( (m_inputRecord = m_srcStream.next()) == null ) {
					return null;
				}
				
				m_transformeds = m_transform.apply(m_inputRecord);
			}
		}
	}
}
