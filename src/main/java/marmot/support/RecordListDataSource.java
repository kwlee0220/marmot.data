package marmot.support;

import java.util.List;

import com.google.common.collect.Lists;

import marmot.Record;
import marmot.RecordReader;
import marmot.RecordSchema;
import marmot.RecordStream;
import marmot.RecordWriter;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class RecordListDataSource {
	public static Reader reader(RecordSchema schema, List<Record> records) {
		return new Reader(schema, records);
	}
	
	public static Writer writer(RecordSchema schema) {
		return new Writer(schema);
	}
	
	private static class Reader implements RecordReader {
		private final RecordSchema m_schema;
		private List<Record> m_records;
		
		private Reader(RecordSchema schema, List<Record> records) {
			m_schema = schema;
			m_records = records;
		}

		@Override
		public RecordSchema getRecordSchema() {
			return m_schema;
		}

		@Override
		public RecordStream read() {
			return RecordStream.from(m_schema, m_records);
		}
	}
	
	public static class Writer implements RecordWriter {
		private final RecordSchema m_schema;
		private List<Record> m_records;
		
		private Writer(RecordSchema schema) {
			m_schema = schema;
			m_records = Lists.newArrayList();
		}
		
		public List<Record> getRecordList() {
			if ( m_records == null ) {
				throw new IllegalStateException("not initialized: no records");
			}
			
			return m_records;
		}

		@Override
		public long write(RecordStream stream) {
			m_records = stream.toList();
			return m_records.size();
		}
	}
}
