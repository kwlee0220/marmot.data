package marmot.csv;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.util.Iterator;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipInputStream;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import marmot.Column;
import marmot.DefaultRecord;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordStreamException;
import marmot.stream.AbstractRecordStream;
import marmot.support.DataUtils;
import marmot.type.DataType;
import utils.StopWatch;
import utils.Utilities;
import utils.func.Try;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class CsvRecordStream extends AbstractRecordStream {
	private static final Logger s_logger = LoggerFactory.getLogger(CsvRecordStream.class);
	
	private final String m_key;
	private final CsvParameters m_options;
	private final CSVParser m_parser;
	private final Iterator<CSVRecord> m_iter;
	private final String m_nullValue;
	private final RecordSchema m_schema;
	private final Column[] m_columns;
	private List<String> m_first;
	private long m_lineNo = 0;
	private StopWatch m_watch;
	private final Record m_record;
	
	public static CsvRecordStream from(String key, InputStream is, CsvParameters opts) throws IOException {
		Utilities.checkNotNullArgument(is, "is is null");
		Utilities.checkNotNullArgument(opts, "CsvOptions is null");
		
		BufferedReader reader = new BufferedReader(new InputStreamReader(is, opts.charset()));
		return new CsvRecordStream(key, reader, opts);
	}
	
	public static CsvRecordStream from(String key, BufferedReader reader, CsvParameters opts)
		throws IOException {
		Utilities.checkNotNullArgument(reader, "reader is null");
		Utilities.checkNotNullArgument(opts, "CsvOptions is null");
		
		return new CsvRecordStream(key, reader, opts);
	}

	public static CsvRecordStream from(File file, CsvParameters params) {
		try {
			@SuppressWarnings("resource")
			InputStream src = new FileInputStream(file);
			String ext = FilenameUtils.getExtension(file.getAbsolutePath());
			switch ( ext ) {
				case "csv":
					break;
				case "gz":
				case "gzip":
					src = new GZIPInputStream(src);
					break;
				case "zip":
					src = new ZipInputStream(src);
					break;
				default:
					String msg = String.format("fails to load SimpleCsvRecordStream: unknown extenstion=%s", ext);
					throw new RecordStreamException(msg);
			}

			Reader reader = new InputStreamReader(new FileInputStream(file), params.charset());
			return new CsvRecordStream(file.getAbsolutePath(), new BufferedReader(reader), params);
		}
		catch ( IOException e ) {
			String msg = String.format("fails to load SimpleCsvRecordStream: %s", file);
			throw new RecordStreamException(msg, e);
		}
	}
	
	private CsvRecordStream(String key, BufferedReader reader, CsvParameters opts) throws IOException {
		m_key = key;
		m_options = opts;
		setLogger(s_logger);
		
		m_watch = StopWatch.start();
		
		m_nullValue = opts.nullValue().getOrNull();
		CSVFormat format = CSVFormat.DEFAULT.withDelimiter(opts.delimiter())
									.withQuote(null);
		format = opts.quote().transform(format, (f,q) -> f.withQuote(q));
		format = opts.escape().transform(format, (f,esc) -> f.withEscape(esc));
		if ( opts.trimColumn() ) {
			format = format.withTrim(true).withIgnoreSurroundingSpaces(true);
		}
		else {
			format = format.withTrim(false).withIgnoreSurroundingSpaces(false);
		}
		m_parser = format.parse(reader);
		
		m_iter = m_parser.iterator();
		if ( !m_iter.hasNext() ) {
			throw new IllegalArgumentException("input CSV file is empty: key=" + m_key);
		}
		m_first = Lists.newArrayList(m_iter.next().iterator());
		
		RecordSchema schema;
		if ( opts.headerFirst() ) {
			schema = CsvUtils.buildRecordSchema(m_first);
			m_first = null;
		}
		else if ( opts.header().isPresent() ) {
			try ( Reader hdrReader = new StringReader(opts.header().getUnchecked());
					CSVParser hdrParser = format.parse(hdrReader); ) {
				CSVRecord header = hdrParser.getRecords().get(0);
				schema = CsvUtils.buildRecordSchema(Lists.newArrayList(header.iterator()));
			}
		}
		else {
			List<String> header = FStream.range(0, m_first.size())
										.map(idx -> String.format("field_%02d", idx))
										.toList();
			schema = CsvUtils.buildRecordSchema(header);
		}
		m_schema = schema;
		m_columns = m_schema.getColumns().toArray(new Column[0]);
		m_record = DefaultRecord.of(m_schema);
	}
	
	@Override
	protected void closeInGuard() {
		Try.run(m_parser::close);
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_schema;
	}
	
	@Override
	public Record next() {
		checkNotClosed();
		
		if ( m_first != null ) {
			++m_lineNo;
			set(m_record, m_first);
			m_first = null;
			
			return m_record;
		}
		
		try {
			if ( !m_iter.hasNext() ) {
				m_watch.stop();
				
				if ( getLogger().isInfoEnabled() ) {
					double velo = m_lineNo / m_watch.getElapsedInFloatingSeconds();
					String msg = String.format("loaded: file=%s, lines=%d, elapsed=%s, velo=%.1f/s",
												m_key, m_lineNo, m_watch.getElapsedSecondString(), velo);
					getLogger().info(msg);
				}
				
				return null;
			}
			
			++m_lineNo;
			List<String> values = Lists.newArrayList(m_iter.next().iterator());
			set(m_record, values);
			
			return m_record;
		}
		catch ( Exception e ) {
			throw new RecordStreamException("line=" + m_lineNo + ": " + e);
		}
	}
	
	@Override
	public String toString() {
		return String.format("%s[%s]", getClass().getSimpleName(), m_options);
	}
	
	private void set(Record output, List<String> values) {
		for ( int i =0; i < Math.min(output.getColumnCount(), values.size()); ++i ) {
			String value = values.get(i);

			// 길이 0 문자열을 null로 간주한다.
			// 이 경우 'null_value' 옵션이 설정된 경우 해당 값으로 치환시킨다.
			if ( value.length() == 0 ) {
				value = m_nullValue;
			}
			if ( m_columns[i].type() != DataType.STRING ) {
				output.set(i, DataUtils.cast(value, m_columns[i].type()));
			}
			else {
				output.set(i, value);
			}
		}
	}
}