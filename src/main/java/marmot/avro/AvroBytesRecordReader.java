package marmot.avro;

import java.io.IOException;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class AvroBytesRecordReader extends AvroRecordReader {
	private byte[] m_bytes;
	
	public AvroBytesRecordReader(byte[] bytes) {
		m_bytes = bytes;
	}
	
	public byte[] getBytes() {
		return m_bytes;
	}
	
	@Override
	protected DataFileReader<GenericRecord> getFileReader() throws IOException {
		SeekableInput input = new SeekableByteArrayInput(m_bytes);
		
		DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>();
		return new DataFileReader<>(input, reader);
	}
}