package marmot.avro;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import marmot.RecordSchema;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class AvroFileRecordWriter extends AvroRecordWriter {
	private static final Logger s_logger = LoggerFactory.getLogger(AvroFileRecordWriter.class);

	private final File m_file;
	private final RecordSchema m_schema;
	private final Schema m_avroSchema;
	
	public AvroFileRecordWriter(File file, RecordSchema schema, Schema avroSchema) {
		m_file = file;
		m_schema = schema;
		m_avroSchema = avroSchema;
		
		setLogger(s_logger);
	}
	
	public AvroFileRecordWriter(File file, RecordSchema schema) {
		this(file, schema, AvroUtils.toSchema(schema));
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_schema;
	}
	
	public File getFile() {
		return m_file;
	}
	
	@Override
	public String toString() {
		return String.format("AvroFileWriter[%s]", m_file);
	}

	@Override
	protected DataFileWriter<GenericRecord> getFileWriter() throws IOException {
		FileUtils.forceMkdirParent(m_file);
		
		GenericDatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(m_avroSchema);
		DataFileWriter<GenericRecord> writer = new DataFileWriter<>(datumWriter);
		writer.setMeta("marmot_schema", m_schema.toString());
		getSyncInterval().transform(writer, DataFileWriter::setSyncInterval);
		getCodec().transform(writer, DataFileWriter::setCodec);
		writer.create(m_avroSchema, m_file);
		
		return writer;
	}
}