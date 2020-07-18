package marmot.avro;

import java.util.Arrays;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import marmot.Column;
import marmot.Record;
import marmot.RecordSchema;
import marmot.type.DataType;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class AvroRecord implements Record {
	private final RecordSchema m_schema;
	private GenericRecord m_grecord;
	private final Object[] m_cache;
	
	AvroRecord(RecordSchema schema, GenericRecord record) {
		m_schema = schema;
		m_grecord = record;
		m_cache = new Object[m_schema.getColumnCount()];
	}
	
	AvroRecord(RecordSchema schema, Schema avroSchema) {
		m_schema = schema;
		m_grecord = new GenericData.Record(avroSchema);
		m_cache = new Object[m_schema.getColumnCount()];
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_schema;
	}
	
	public GenericRecord getGenericRecord() {
		return m_grecord;
	}
	
	public void setGenericRecord(GenericRecord grec) {
		m_grecord = grec;
		Arrays.fill(m_cache, null);
	}
	
	public void clearCache() {
		Arrays.fill(m_cache, null);
	}

	@Override
	public Object get(int index) {
		Column col = m_schema.getColumnAt(index);
		if ( col.type().isGeometryType() ) {
			if ( m_cache[index] == null ) {
				m_cache[index] = AvroUtils.fromAvroValue(col.type(), m_grecord.get(index));
			}
			return m_cache[index];		
		}
		else {
			return m_grecord.get(index);
		}
	}

	@Override
	public Record set(int idx, Object value) {
		DataType type = m_schema.getColumnAt(idx).type();
		if ( type.isGeometryType() ) {
			m_cache[idx] = value;
		}
		
		Object field = AvroUtils.toAvroValue(type, value);
		m_grecord.put(idx,  field);
		return this;
	}
	
	@Override
	public String toString() {
		return "" + m_grecord;
	}
}
