package marmot.remote.client;

import marmot.DataSet;
import marmot.DataSetInfo;
import marmot.RecordSchema;
import marmot.RecordStream;
import marmot.avro.AvroDeserializer;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GrpcDataSetProxy implements DataSet {
	private final GrpcDataSetServiceProxy m_service;
	private final String m_id;
	private final DataSetInfo m_info;
	
	GrpcDataSetProxy(GrpcDataSetServiceProxy service, String id, DataSetInfo info) {
		m_service = service;
		m_id = id;
		m_info = info;
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_info.getRecordSchema();
	}

	@Override
	public RecordStream read() {
		return AvroDeserializer.from(m_info.getRecordSchema(), m_service.readDataSet(m_id));
	}
	
	@Override
	public String toString() {
		return String.format("%s[%s]", getClass().getSimpleName(), m_id);
	}
}
