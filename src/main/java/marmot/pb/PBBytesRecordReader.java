package marmot.pb;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import marmot.RecordSchema;
import proto.StringProto;
import utils.func.Tuple;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PBBytesRecordReader extends PBRecordReader {
	private final byte[] m_bytes;
	
	public PBBytesRecordReader(byte[] bytes) {
		m_bytes = bytes;
	}
	
	public byte[] getBytes() {
		return m_bytes;
	}
	
	@Override
	protected Tuple<RecordSchema, InputStream> getInputStream() throws IOException {
		if ( m_bytes == null ) {
			throw new IllegalStateException("dataset bytes is not present");
		}
		InputStream is = new ByteArrayInputStream(m_bytes);
		
		String typeId = StringProto.parseDelimitedFrom(is).getValue();
		RecordSchema schema = RecordSchema.fromTypeId(typeId);
		
		return Tuple.of(schema, is);
	}
}