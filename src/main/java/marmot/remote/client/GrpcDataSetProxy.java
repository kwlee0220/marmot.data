package marmot.remote.client;

import java.io.IOException;
import java.io.InputStream;

import org.apache.avro.Schema;

import marmot.RecordSchema;
import marmot.RecordStream;
import marmot.RecordStreamException;
import marmot.avro.AvroBinaryRecordReader;
import marmot.avro.AvroUtils;
import marmot.dataset.AbstractDataSet;
import marmot.dataset.DataSetInfo;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GrpcDataSetProxy extends AbstractDataSet<GrpcDataSetServerProxy> {
	private final Schema m_avroSchema;
	
	GrpcDataSetProxy(GrpcDataSetServerProxy service, DataSetInfo info) {
		super(service, info);
		
		m_avroSchema = AvroUtils.toSchema(info.getRecordSchema());
	}

	@Override
	public RecordStream read() {
		RecordSchema schema = m_info.getRecordSchema();
		InputStream is = m_server.readDataSet(m_info.getId());
		
		try {
			return AvroBinaryRecordReader.deserialize(schema, m_avroSchema, is);
		}
		catch ( IOException e ) {
			throw new RecordStreamException("" + e);
		}
	}

	@Override
	public long write(RecordStream stream) {
		m_info = m_server.writeDataSet2(m_info.getId(), stream);
		return m_info.getRecordCount();
	}

	@Override
	public void append(RecordStream stream) {
		throw new UnsupportedOperationException();
	}
	
	@Override
	public String toString() {
		return String.format("%s[%s]", getClass().getSimpleName(), m_info.getId());
	}

	@Override
	public long getLength() {
		return 0;
	}
}
