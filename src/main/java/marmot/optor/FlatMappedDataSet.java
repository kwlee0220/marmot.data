package marmot.optor;

import java.util.function.Function;

import marmot.DataSet;
import marmot.DefaultRecord;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordStream;
import marmot.stream.AbstractRecordStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class FlatMappedDataSet implements DataSet {
	private final DataSet m_srcDataSet;
	private final RecordSchema m_outSchema;
	private final Function<? super Record,RecordStream> m_transform;
	
	public FlatMappedDataSet(DataSet input, RecordSchema outSchema,
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
		
		private final Record m_inputRecord;
		private RecordStream m_transformeds;
		
		StreamImpl(RecordStream input) {
			m_srcStream = input;
			
			m_inputRecord = DefaultRecord.of(input.getRecordSchema());
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
		public boolean next(Record record) {
			while ( true ) {
				if ( m_transformeds.next(record) ) {
					return true;
				}
				
				if ( !m_srcStream.next(m_inputRecord) ) {
					return false;
				}
				
				m_transformeds = m_transform.apply(m_inputRecord);
			}
		}
	}
}
