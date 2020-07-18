package marmot.avro;

import java.io.File;
import java.io.IOException;

import javax.annotation.Nullable;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableFileInput;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

import marmot.RecordReader;
import marmot.RecordSchema;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class AvroFileRecordReader extends AvroRecordReader {
	private final File m_file;
	
	public AvroFileRecordReader(File file) {
		m_file = file;
	}
	
	public File getFile() {
		return m_file;
	}
	
	@Override
	protected DataFileReader<GenericRecord> getFileReader() throws IOException {
		SeekableInput input = new SeekableFileInput(m_file);
		
		DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>();
		return new DataFileReader<>(input, reader);
	}
}