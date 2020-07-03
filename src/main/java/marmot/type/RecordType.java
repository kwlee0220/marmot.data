package marmot.type;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.commons.compress.utils.Lists;

import marmot.Column;
import marmot.DefaultRecord;
import marmot.Record;
import marmot.RecordSchema;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class RecordType extends DataType {
	private static final long serialVersionUID = 1L;
	static RecordType NULL = new RecordType(RecordSchema.NULL);
	
	private final RecordSchema m_schema;
	private final String m_displayName;
	
	RecordType(RecordSchema schema) {
		super(encodeTypeId(schema), TypeClass.RECORD, Record.class);
		
		m_schema = schema;
		m_displayName = toDisplayName(schema);
	}
	
	public RecordSchema getRecordSchema() {
		return m_schema;
	}

	@Override
	public String displayName() {
		return m_displayName;
	}

	@Override
	public Object newInstance() {
		return Lists.newArrayList();
	}

	@Override
	public Object parseInstance(String str) {
		throw new UnsupportedOperationException("type=" + this + ", str=" + str);
	}

	@Override
	public void serialize(Object obj, ObjectOutputStream oos) throws IOException {
		if ( obj instanceof Record ) {
			Record record = (Record)obj;
			
			for ( int i =0; i < m_schema.getColumnCount(); ++i ) {
				Column col = m_schema.getColumnAt(i);
				Object value = record.get(i);
				
				col.type().serialize(value, oos);
			}
		}
		else {
			throw new IOException("input stream corrupted: not " + getClass());
		}
	}

	@Override
	public Object deserialize(ObjectInputStream ois) throws IOException {
		Record record = DefaultRecord.of(m_schema);
		m_schema.streamColumns()
				.map(Column::type)
				.zipWithIndex()
				.forEachOrThrow(t -> record.set(t._2, t._1.deserialize(ois)));
		return record;
	}
	
	private static String encodeTypeId(RecordSchema schema) {
		return schema.streamColumns()
					.map(col -> String.format("%s:%s", col.name(), col.type().id()))
					.join(",", "{", "}");
	}
	
	private static String toDisplayName(RecordSchema schema) {
		return schema.streamColumns()
					.map(col -> String.format("%s:%s", col.name(), col.type()))
					.join(",", "record{", "}");
	}
}
