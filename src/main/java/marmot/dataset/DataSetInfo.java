package marmot.dataset;

import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.Serializable;

import javax.annotation.Nullable;

import org.locationtech.jts.geom.Envelope;

import marmot.DefaultRecord;
import marmot.Record;
import marmot.RecordSchema;
import marmot.dataset.proto.DataSetInfoProto;
import marmot.dataset.proto.DataSetTypeProto;
import marmot.pb.PBDataSetUtils;
import marmot.remote.PBSerializable;
import marmot.type.DataType;
import utils.Utilities;
import utils.func.FOption;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class DataSetInfo implements PBSerializable<DataSetInfoProto>, Serializable {
	private static final long serialVersionUID = 1L;
	public static final RecordSchema SCHEMA = RecordSchema.builder()
															.addColumn("id", DataType.STRING)
															.addColumn("type", DataType.STRING)
															.addColumn("schema", DataType.STRING)
															.addColumn("count", DataType.LONG)
															.addColumn("bounds", DataType.ENVELOPE)
															.addColumn("parameter", DataType.STRING)
															.addColumn("updated_millis", DataType.LONG)
															.build();
	private static final Envelope EMPTY = new Envelope();

	private final String m_id;
	private final DataSetType m_type;
	private final RecordSchema m_schema;
	private @Nullable Envelope m_bounds;
	private long m_count = 0;
	private String m_parameter;
	private long m_updateEpochMillis;
	
	public DataSetInfo(String id, DataSetType type, RecordSchema schema) {
		Utilities.checkNotNullArgument(schema, "DataSet's RecordSchema should not be null.");
		
		m_id = id;
		m_type = type;
		m_schema = schema;
		m_count = 0;
		m_bounds = null;
	}
	
	public String getId() {
		return m_id;
	}
	
	public RecordSchema getRecordSchema() {
		return m_schema;
	}
	
	public DataSetType getType() {
		return m_type;
	}
	
	public Envelope getBounds() {
		return FOption.ofNullable(m_bounds).getOrElse(EMPTY);
	}
	
	public void setBounds(Envelope bounds) {
		m_bounds = (bounds == null || bounds.isNull()) ? null : bounds;
	}
	
	public long getRecordCount() {
		return m_count;
	}
	
	public void setRecordCount(long count) {
		m_count = count;
	}
	
	public String getParameter() {
		return m_parameter;
	}
	
	public void setParameter(String param) {
		m_parameter = param;
	}
	
	public long getUpdateEpochMillis() {
		return m_updateEpochMillis;
	}
	
	public void setUpdateEpochMillis(long millis) {
		m_updateEpochMillis = millis;
	}
	
	public DataSetInfo clone(String dsId) {
		DataSetInfo copied = new DataSetInfo(dsId, m_type, m_schema);
		copied.setRecordCount(m_count);
		copied.setBounds(m_bounds);
		copied.setParameter(m_parameter);
		copied.setUpdateEpochMillis(m_updateEpochMillis);
		
		return copied;
	}
	
	public DataSetInfo duplicate() {
		DataSetInfo copied = new DataSetInfo(m_id, m_type, m_schema);
		copied.setRecordCount(m_count);
		copied.setBounds(m_bounds);
		copied.setParameter(m_parameter);
		copied.setUpdateEpochMillis(m_updateEpochMillis);
		
		return copied;
	}
	
	public static DataSetInfo fromProto(DataSetInfoProto proto) {
		String id = proto.getId();
		DataSetType type = DataSetType.valueOf(proto.getType().name());
		RecordSchema schema = RecordSchema.fromTypeId(proto.getRecordSchema());
		
		DataSetInfo dsInfo = new DataSetInfo(id, type, schema);
		dsInfo.setRecordCount(proto.getCount());
		dsInfo.setParameter(proto.getParameter());
		
		switch ( proto.getOptionalBoundsCase() ) {
			case BOUNDS:
				dsInfo.setBounds(PBDataSetUtils.fromProto(proto.getBounds()));
				break;
			case OPTIONALBOUNDS_NOT_SET:
				dsInfo.setBounds(null);
				break;
		}
		
		return dsInfo;
	}

	@Override
	public DataSetInfoProto toProto() {
		DataSetInfoProto.Builder builder = DataSetInfoProto.newBuilder()
															.setId(m_id)
															.setType(DataSetTypeProto.valueOf(m_type.name()))
															.setRecordSchema(m_schema.toTypeId())
															.setCount(m_count)
															.setParameter(m_parameter);
		if ( m_bounds != null ) {
			builder.setBounds(PBDataSetUtils.toProto(m_bounds));
		}
		
		return builder.build();
	}
	
	@Override
	public String toString() {
		return String.format("%s[id=%s, schema=%s, count=%d]", m_type, m_id, m_schema, m_count);
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		else if ( obj == null || getClass() != obj.getClass() ) {
			return false;
		}
		
		DataSetInfo other = (DataSetInfo)obj;
		return m_id.equals(other.m_id);
	}
	
	@Override
	public int hashCode() {
		return m_id.hashCode();
	}
	
	public static DataSetInfo fromRecord(Record record) {
		DataSetType type = DataSetType.valueOf(record.getString(1));
		RecordSchema schema = RecordSchema.fromTypeId(record.getString(2));
		DataSetInfo dsInfo = new DataSetInfo(record.getString(0), type, schema);
		dsInfo.setRecordCount(record.getLong(3));
		dsInfo.setBounds((Envelope)record.get(4));
		dsInfo.setParameter(record.getString(5));
		dsInfo.setUpdateEpochMillis(record.getLong(6));
		
		return dsInfo;
	}
	
	public void copyToRecord(Record output) {
		Record record = DefaultRecord.of(SCHEMA);
		record.set(0, m_id);
		record.set(1, m_type.name());
		record.set(2, m_schema.toTypeId());
		record.set(3, m_count);
		record.set(4, m_bounds);
		record.set(5, m_updateEpochMillis);
	}
	
	public Record toRecord() {
		Record record = DefaultRecord.of(SCHEMA);
		copyToRecord(record);
		
		return record;
	}
	
	private Object writeReplace() {
		return new SerializationProxy(this);
	}
	
	private void readObject(ObjectInputStream stream) throws InvalidObjectException {
		throw new InvalidObjectException("Use Serialization Proxy instead.");
	}

	private static class SerializationProxy implements Serializable {
		private static final long serialVersionUID = 1L;
		
		private final DataSetInfoProto m_proto;
		
		private SerializationProxy(DataSetInfo info) {
			m_proto = info.toProto();
		}
		
		private Object readResolve() {
			return DataSetInfo.fromProto(m_proto);
		}
	}
}