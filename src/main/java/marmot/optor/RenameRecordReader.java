package marmot.optor;

import java.util.Map;

import com.google.common.collect.Maps;

import marmot.Column;
import marmot.Record;
import marmot.RecordReader;
import marmot.RecordSchema;
import marmot.RecordStream;
import marmot.stream.AbstractRecordStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class RenameRecordReader implements RecordReader {
	private final RecordReader m_input;
	private final RecordSchema m_newSchema;
	
	public RenameRecordReader(RecordReader input, Map<String,String> map) {
		m_input = input;
		
		Map<String,String> remains = Maps.newHashMap(map);
		m_newSchema = input.getRecordSchema().streamColumns()
							.map(c -> {
								String newName = remains.remove(c.name());
								if ( newName != null ) {
									return new Column(newName, c.type());
								}
								else {
									return c;
								}
							})
							.collect(RecordSchema.builder(), (b,c) -> b.addColumn(c))
							.build();
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_newSchema;
	}

	@Override
	public RecordStream read() {
		return new StreamImpl(m_input.read(), m_newSchema);
	}

	public class StreamImpl extends AbstractRecordStream {
		private final RecordStream m_src;
		private final RecordSchema m_newSchema;
		
		public StreamImpl(RecordStream src, RecordSchema newSchema) {
			m_src = src;
			m_newSchema = newSchema;
		}
	
		@Override
		protected void closeInGuard() throws Exception {
			m_src.close();
		}
	
		@Override
		public RecordSchema getRecordSchema() {
			return m_newSchema;
		}
	
		@Override
		public Record next() {
			return m_src.next();
		}
	
		@Override
		public Record nextCopy() {
			return m_src.nextCopy();
		}
	}
}
