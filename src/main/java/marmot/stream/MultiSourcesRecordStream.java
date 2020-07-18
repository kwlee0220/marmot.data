package marmot.stream;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import marmot.RecordSchema;
import marmot.RecordStream;
import marmot.RecordStreamException;
import utils.Utilities;
import utils.io.FileUtils;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class MultiSourcesRecordStream<T> extends ChainedRecordStream {
	private static final Logger s_logger = LoggerFactory.getLogger(MultiSourcesRecordStream.class);
	
	private final FStream<T> m_sources;
	private final RecordSchema m_schema;
	private RecordStream m_first;
	
	abstract protected RecordStream read(T source) throws RecordStreamException;
	
	protected MultiSourcesRecordStream(FStream<T> sources, RecordSchema schema) {
		Utilities.checkNotNullArgument(sources, "sources is null");
		
		m_sources = sources;
		m_first = m_sources.next().mapOrThrow(this::read).getOrNull();
		m_schema = schema != null ? schema : m_first.getRecordSchema();
		
		setLogger(s_logger);
	}
	
	@Override
	protected void closeInGuard() throws Exception {
		if ( m_first != null ) {
			m_first.closeQuietly();
			m_first = null;
		}
		m_sources.closeQuietly();
		
		super.closeInGuard();
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_schema;
	}
	
	@Override
	public String toString() {
		return String.format("%s", getClass().getSimpleName());
	}

	@Override
	protected RecordStream getNextRecordStream() {
		if ( m_first != null ) {
			RecordStream strm = m_first;
			m_first = null;
			
			return strm;
		}
		else {
			return m_sources.next()
							.ifPresent(source -> getLogger().debug("loading: file: {}", source))
							.map(this::read)
							.getOrNull();
		}
	}

	static List<File> collectFiles(File start, String glob) throws IOException {
		Utilities.checkNotNullArgument(start, "start is null");
		Utilities.checkNotNullArgument(glob, "glob is null");
		
		List<File> files = (start.isDirectory()) ? FileUtils.walk(start, glob).toList()
												: Collections.singletonList(start);
		s_logger.debug("loading files: from={}, nfiles={}", start, files.size());
		
		return files;
	}
}
