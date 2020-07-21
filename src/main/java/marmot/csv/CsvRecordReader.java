package marmot.csv;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import marmot.RecordReader;
import marmot.RecordSchema;
import marmot.RecordStream;
import marmot.RecordStreamException;
import marmot.stream.MultiSourcesRecordStream;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class CsvRecordReader implements RecordReader {
	private static final Logger s_logger = LoggerFactory.getLogger(CsvRecordReader.class);
	
	private final File m_start;
	private final CsvParameters m_params;
	private final String m_glob;
	
	private final List<File> m_csvFiles;
	private final RecordSchema m_schema;
	
	public static CsvRecordReader from(File start, CsvParameters params) {
		return from(start, params, "**/*.csv");
	}
	
	public static CsvRecordReader from(File start, CsvParameters params, String glob) {
		return new CsvRecordReader(start, params, glob);
	}
	
	private CsvRecordReader(File start, CsvParameters params, String glob) {
		m_start = start;
		m_params = params;
		m_glob = glob;
		
		try {
			m_csvFiles = MultiSourcesRecordStream.collectFiles(start, m_glob);
			if ( m_csvFiles.isEmpty() ) {
				throw new IllegalArgumentException("no CSV files to read: start=" + start);
			}
			
			try ( RecordStream strm = CsvRecordStream.from(m_csvFiles.get(0), m_params) ) {
				m_schema = strm.getRecordSchema();
			}
			
			s_logger.info("loading {}: nfiles={}", this, m_csvFiles.size());
		}
		catch ( IOException e ) {
			throw new RecordStreamException("fails to load ShapefileRecordStream: start=" + start,  e);
		}
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_schema;
	}

	@Override
	public RecordStream read() {
		return new StreamImpl(m_csvFiles, m_schema, m_params);
	}
	
	@Override
	public String toString() {
		return String.format("%s[start=%s]params[%s]", getClass().getSimpleName(), m_start, m_params);
	}

	static class StreamImpl extends MultiSourcesRecordStream<File> {
		private static final Logger s_logger = LoggerFactory.getLogger(StreamImpl.class);
		
		private final CsvParameters m_params;
		
		StreamImpl(List<File> files, RecordSchema schema, CsvParameters params) {
			super(FStream.from(files), schema);
			
			m_params = params;
			setLogger(s_logger);
		}

		@Override
		protected RecordStream read(File file) throws RecordStreamException {
			return CsvRecordStream.from(file, m_params);
		}
		
		@Override
		public String toString() {
			return String.format("%s: params[%s]", getClass().getSimpleName(), m_params);
		}
	}
}
