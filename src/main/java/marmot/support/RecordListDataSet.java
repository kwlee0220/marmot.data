package marmot.support;

import java.util.List;

import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordStream;
import marmot.WritableDataSet;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class RecordListDataSet implements WritableDataSet {
	private final RecordSchema m_schema;
	private List<Record> m_records;
	
	public static RecordListDataSet of(RecordSchema schema) {
		return new RecordListDataSet(schema, null);
	}
	
	public static RecordListDataSet of(RecordSchema schema, List<Record> records) {
		return new RecordListDataSet(schema, records);
	}
	
	private RecordListDataSet(RecordSchema schema, List<Record> records) {
		m_schema = schema;
		m_records = records;
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_schema;
	}
	
	public List<Record> getRecordList() {
		if ( m_records == null ) {
			throw new IllegalStateException("not initialized: no records");
		}
		
		return m_records;
	}

	@Override
	public RecordStream read() {
		return RecordStream.from(m_schema, m_records);
	}

	@Override
	public long write(RecordStream stream) {
		m_records = stream.toList();
		return m_records.size();
	}
}
