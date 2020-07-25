package marmot.avro;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import marmot.RecordReader;
import marmot.RecordSchema;
import marmot.RecordStream;
import marmot.RecordStreamException;
import marmot.stream.MultiSourcesRecordStream;
import utils.Utilities;
import utils.io.FileUtils;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MultiFileAvroReader implements RecordReader {
	private static final Logger s_logger = LoggerFactory.getLogger(MultiFileAvroReader.class);

	private final File m_start;
	private final String m_glob;
	private final RecordSchema m_schema;
	private final Schema m_avroSchema;
	
	public static MultiFileAvroReader scan(File start, String glob) {
		return new MultiFileAvroReader(start, glob);
	}
	
	public static MultiFileAvroReader scan(File start) {
		return scan(start, "**/*.avro");
	}
	
	private MultiFileAvroReader(File start, String glob) {
		m_start = start;
		m_glob = glob;
		
		m_avroSchema = readAvroSchema(start);
		m_schema = AvroUtils.toRecordSchema(m_avroSchema);
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_schema;
	}

	@Override
	public RecordStream read() {
		return new StreamImpl(FStream.from(collectFiles(m_start, m_glob)));
	}
	
	@Override
	public String toString() {
		return String.format("%s[start=%s]", getClass().getSimpleName(), m_start);
	}
	
	class StreamImpl extends MultiSourcesRecordStream<File> {
		StreamImpl(FStream<File> paths) {
			super(paths, m_schema);
		}

		@Override
		protected RecordStream read(File file) throws RecordStreamException {
			return new AvroFileRecordReader(file).read();
		}
	}

	public static List<File> collectFiles(File start, String glob) {
		Utilities.checkNotNullArgument(start, "start is null");
		Utilities.checkNotNullArgument(glob, "glob is null");
		
		try {
			List<File> files = (start.isDirectory()) ? FileUtils.walk(start, glob).toList()
													: Collections.singletonList(start);
			s_logger.debug("loading files: from={}, nfiles={}", start, files.size());
			
			return files;
		}
		catch ( IOException e ) {
			throw new RecordStreamException("fails to collect files: start=" + start, e);
		}
	}
	
	private static Schema readAvroSchema(File file) {
		File schemaFile = new File(file, "_schema.avsc");
		try ( InputStream is = new FileInputStream(schemaFile) ) {
			Schema.Parser parser = new Schema.Parser();
			return parser.parse(is);
		}
		catch ( IOException e ) {
			throw new RecordStreamException("fails to read avro schema file: " + schemaFile, e);
		}
	}
}
