package marmot.stream;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import marmot.RecordSchema;
import marmot.RecordStream;
import marmot.RecordStreamException;
import utils.StopWatch;
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
	private @Nullable T m_current;
	private StopWatch m_watch;
	
	abstract protected RecordStream read(T source) throws RecordStreamException;
	
	protected MultiSourcesRecordStream(FStream<T> sources, RecordSchema schema) {
		Utilities.checkNotNullArgument(sources, "sources is null");
		Utilities.checkNotNullArgument(schema, "RecordSchema is null");
		
		m_sources = sources;
		m_schema = schema;
		m_first = null;
		m_current = null;
		
		setLogger(s_logger);
	}
	
	protected MultiSourcesRecordStream(FStream<T> sources) {
		Utilities.checkNotNullArgument(sources, "sources is null");
		
		m_sources = sources;
		m_current = m_sources.next()
							.getOrThrow(() -> new IllegalArgumentException("Cannot get a RecordSchema from empty "
																			+ "MultiSourcesRecordStream"));
		m_first = read(m_current);
		m_schema = m_first.getRecordSchema();
		
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
			m_watch = StopWatch.start();
			
			return strm;
		}
		else {
			if ( m_current != null ) {
				getLogger().info("loaded: {}, elapsed={}", m_current, m_watch.stopAndGetElpasedTimeString());
			}
			
			return m_sources.next()
							.ifPresent(source -> {
								m_current = source;
								m_watch = StopWatch.start();
								getLogger().debug("loading: {}", source);
							})
							.map(this::read)
							.getOrNull();
		}
	}

	public static List<File> collectFiles(File start, String glob) throws IOException {
		Utilities.checkNotNullArgument(start, "start is null");
		Utilities.checkNotNullArgument(glob, "glob is null");
		
		List<File> files = (start.isDirectory()) ? FileUtils.walk(start, glob).toList()
												: Collections.singletonList(start);
		s_logger.debug("loading files: from={}, nfiles={}", start, files.size());
		
		return files;
	}
}
