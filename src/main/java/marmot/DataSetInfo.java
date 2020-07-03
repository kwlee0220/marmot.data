package marmot;

import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.Serializable;

import marmot.remote.PBSerializable;
import proto.dataset.DataSetInfoProto;
import utils.Utilities;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class DataSetInfo implements PBSerializable<DataSetInfoProto>, Serializable {
	private static final long serialVersionUID = 1L;
	
	private final RecordSchema m_schema;
	
	public DataSetInfo(RecordSchema schema) {
		Utilities.checkNotNullArgument(schema, "DataSet's RecordSchema should not be null.");
		
		m_schema = schema;
	}
	
	public RecordSchema getRecordSchema() {
		return m_schema;
	}
	
	public static DataSetInfo fromProto(DataSetInfoProto proto) {
		return new DataSetInfo(RecordSchema.fromTypeId(proto.getRecordSchema()));
	}

	@Override
	public DataSetInfoProto toProto() {
		return DataSetInfoProto.newBuilder()
								.setRecordSchema(m_schema.toTypeId())
								.build();
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