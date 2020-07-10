package marmot.remote.client;

import java.io.IOException;
import java.io.InputStream;

import marmot.RecordSchema;
import marmot.RecordStream;
import marmot.RecordStreamException;
import marmot.avro.AvroDeserializer;
import marmot.dataset.DataSet;
import marmot.dataset.DataSetInfo;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GrpcDataSetProxy implements DataSet {
	private final GrpcDataSetServerProxy m_service;
	private DataSetInfo m_info;
	
	GrpcDataSetProxy(GrpcDataSetServerProxy service, DataSetInfo info) {
		m_service = service;
		m_info = info;
	}

	@Override
	public DataSetInfo getDataSetInfo() {
		return m_info;
	}

	@Override
	public RecordStream read() {
		RecordSchema schema = m_info.getRecordSchema();
		InputStream is = m_service.readDataSet(m_info.getId());
		
		try {
			return AvroDeserializer.deserialize(schema, is);
		}
		catch ( IOException e ) {
			throw new RecordStreamException("" + e);
		}
	}

	@Override
	public void write(RecordStream stream) {
		m_info = m_service.writeDataSet2(m_info.getId(), stream);
	}

	@Override
	public void append(RecordStream stream) {
		throw new UnsupportedOperationException();
	}
	
	@Override
	public String toString() {
		return String.format("%s[%s]", getClass().getSimpleName(), m_info.getId());
	}
}
