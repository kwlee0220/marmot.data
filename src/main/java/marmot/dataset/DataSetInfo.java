package marmot.dataset;

import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.Serializable;

import javax.annotation.Nullable;

import com.vividsolutions.jts.geom.Envelope;

import marmot.RecordSchema;
import marmot.pb.PBDataSetUtils;
import marmot.remote.PBSerializable;
import proto.dataset.DataSetInfoProto;
import utils.Utilities;
import utils.func.FOption;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class DataSetInfo implements PBSerializable<DataSetInfoProto>, Serializable {
	private static final long serialVersionUID = 1L;
	private static final Envelope EMPTY = new Envelope();

	private final String m_id;
	private final RecordSchema m_schema;
	@Nullable private Envelope m_bounds;
	private long m_count = 0;
	
	private long m_updateEpochMillis;
	
	public DataSetInfo(String id, RecordSchema schema) {
		Utilities.checkNotNullArgument(schema, "DataSet's RecordSchema should not be null.");
		
		m_id = id;
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
	
	public long getUpdateEpochMillis() {
		return m_updateEpochMillis;
	}
	
	public static DataSetInfo fromProto(DataSetInfoProto proto) {
		String id = proto.getId();
		RecordSchema schema = RecordSchema.fromTypeId(proto.getRecordSchema());
		
		DataSetInfo dsInfo = new DataSetInfo(id, schema);
		dsInfo.setRecordCount(proto.getCount());
		
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
															.setRecordSchema(m_schema.toTypeId())
															.setCount(m_count);
		if ( m_bounds != null ) {
			builder.setBounds(PBDataSetUtils.toProto(m_bounds));
		}
		
		return builder.build();
	}
	
	@Override
	public String toString() {
		return String.format("%s[%s, count=%d]", m_id, m_schema, m_count);
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