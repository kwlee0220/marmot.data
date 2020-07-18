package marmot.csv;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import marmot.RecordSchema;
import marmot.RecordStreamException;
import marmot.stream.ChainedRecordStream;
import utils.Utilities;
import utils.io.FileUtils;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MultiFileCsvRecordStream extends ChainedRecordStream {
	private static final Logger s_logger = LoggerFactory.getLogger(MultiFileCsvRecordStream.class);
	
	private final File m_start;
	private final CsvParameters m_params;
	private final FStream<File> m_files;
	private CsvRecordStream m_first;
	private final RecordSchema m_schema;
	
	MultiFileCsvRecordStream(File start, CsvParameters params) {
		this(start, params, "**/*.csv");
	}
	
	MultiFileCsvRecordStream(File start, CsvParameters params, String glob) {
		Utilities.checkNotNullArgument(start, "start is null");
		Utilities.checkNotNullArgument(params, "params is null");
		Utilities.checkNotNullArgument(glob, "glob is null");
		
		m_start = start;
		m_params = params;
		setLogger(s_logger);
		
		try {
			List<File> files = collectCsvFiles(m_start, params, glob);
			getLogger().debug("loading CSVFile: from={}, nfiles={}", start, files.size());

			m_files = FStream.from(files);
			m_first = getNextRecordStream();
			m_schema = m_first.getRecordSchema();
		}
		catch ( IOException e ) {
			throw new RecordStreamException("fails to parse CSV, cause=" + e);
		}
	}
	
	@Override
	protected void closeInGuard() throws Exception {
		if ( m_first != null ) {
			m_first.closeQuietly();
			m_first = null;
		}
		m_files.closeQuietly();
		
		super.closeInGuard();
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_schema;
	}

	static List<File> collectCsvFiles(File start, CsvParameters params, String glob) throws IOException {
		Utilities.checkNotNullArgument(start, "start is null");
		Utilities.checkNotNullArgument(params, "params is null");
		Utilities.checkNotNullArgument(glob, "glob is null");
		
		List<File> files;
		if ( start.isDirectory() ) {
			files = FileUtils.walk(start, glob)
							.sort()
							.toList();
			if ( files.isEmpty() ) {
				throw new IllegalArgumentException("no CSV files to read: path=" + start);
			}
		}
		else {
			files = Lists.newArrayList(start);
		}
		if ( files.isEmpty() ) {
			throw new IllegalArgumentException("no CSV files to read: path=" + start);
		}
		s_logger.debug("loading CSVFile: from={}, nfiles={}", start, files.size());
		
		return files;
	}
	
	@Override
	public String toString() {
		return String.format("%s[start=%s]params[%s]", getClass().getSimpleName(), m_start, m_params);
	}

	@Override
	protected CsvRecordStream getNextRecordStream() {
		if ( m_first != null ) {
			CsvRecordStream rset = m_first;
			m_first = null;
			
			return rset;
		}
		else {
			return m_files.next()
							.map(file -> {
								getLogger().debug("loading: CSV[{}], {}", m_params, file);
								return CsvRecordStream.from(file, m_params);
							})
							.getOrNull();
		}
	}
}
