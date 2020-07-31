package marmot.pb;

import static utils.grpc.PBUtils.STRING;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import marmot.RecordSchema;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PBBytesRecordWriter extends PBRecordWriter {
	private final RecordSchema m_schema;
	private byte[] m_bytes;
	
	PBBytesRecordWriter(RecordSchema schema) {
		m_schema = schema;
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_schema;
	}
	
	public byte[] getBytes() {
		return m_bytes;
	}

	@Override
	protected OutputStream getOutputStream(RecordSchema schema) throws IOException {
		PBOutputStream os = new PBOutputStream();
		STRING(schema.toTypeId()).writeDelimitedTo(os);
		os.flush();
		return os;
	}
	
	private final class PBOutputStream extends ByteArrayOutputStream {
		@Override
		public void close() throws IOException {
			super.close();
			
			m_bytes = toByteArray();
		}
	}
}
