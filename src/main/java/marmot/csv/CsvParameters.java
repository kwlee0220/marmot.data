package marmot.csv;

import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import utils.Tuple;
import utils.Tuple4;
import utils.Utilities;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@Command(description="CSV Parameters")
public class CsvParameters {
	private char m_delim = ',';
	@Nullable private Character m_quote = null;
	@Nullable private Character m_escape = null;
	@Nullable private String m_comment = null;
	private Charset m_charset = StandardCharsets.UTF_8;
	private boolean m_headerFirst = false;
	@Nullable private String m_header = null;
	@Nullable private String m_pointColsExpr = null;
	@Nullable private Tuple4<String,String,String,String> m_pointCols = null;
	private boolean m_trimColumns = false;
	@Nullable private String m_nullValue = null;
	private boolean m_tiger = false;
	
	public static CsvParameters create() {
		return new CsvParameters();
	}
	
	public char delimiter() {
		return m_delim;
	}

	@Option(names={"--delim"}, paramLabel="char", description={"delimiter for CSV file"})
	public CsvParameters delimiter(Character delim) {
		m_delim = delim;
		return this;
	}
	
	public FOption<Character> quote() {
		return FOption.ofNullable(m_quote);
	}

	@Option(names={"--quote"}, paramLabel="char", description={"quote character for CSV file"})
	public CsvParameters quote(char quote) {
		m_quote = quote;
		return this;
	}
	
	public FOption<Character> escape() {
		return FOption.ofNullable(m_escape);
	}

	@Option(names={"--escape"}, paramLabel="char", description={"quote escape character for CSV file"})
	public CsvParameters escape(char escape) {
		m_escape = escape;
		return this;
	}
	
	public Charset charset() {
		return m_charset;
	}

	@Option(names={"--charset"}, paramLabel="charset-string",
			description={"Character encoding of the target CSV file"})
	public CsvParameters charset(String charset) {
		Utilities.checkNotNullArgument(charset, "charset");
		
		m_charset = Charset.forName(charset);
		return this;
	}
	
	public CsvParameters charset(Charset charset) {
		m_charset = charset;
		return this;
	}
	
	public FOption<String> commentMarker() {
		return FOption.ofNullable(m_comment);
	}

	@Option(names={"--comment"}, paramLabel="comment_marker", description={"comment marker"})
	public CsvParameters commentMarker(String comment) {
		m_comment = comment;
		return this;
	}
	
	public FOption<String> header() {
		return FOption.ofNullable(m_header);
	}

	@Option(names={"--header"}, paramLabel="column_list", description={"header field list"})
	public CsvParameters header(String header) {
		Utilities.checkNotNullArgument(header, "CSV header");
		
		m_header = header;
		return this;
	}
	
	public boolean headerFirst() {
		return m_headerFirst;
	}

	@Option(names={"--header_first"}, description="consider the first line as header")
	public CsvParameters headerFirst(boolean flag) {
		m_headerFirst = flag;
		return this;
	}

	@Option(names={"--null_value"}, paramLabel="string",
			description="null value for column")
	public CsvParameters nullValue(String str) {
		m_nullValue = str;
		return this;
	}
	
	public FOption<String> nullValue() {
		return FOption.ofNullable(m_nullValue);
	}

	@Option(names={"--trim_columns"}, description="ignore surrouding spaces")
	public CsvParameters trimColumns(boolean flag) {
		m_trimColumns = flag;
		return this;
	}
	
	public boolean trimColumn() {
		return m_trimColumns;
	}

	@Option(names={"--point_cols"}, paramLabel="xy-columns", description="X,Y columns for point")
	public CsvParameters pointColumns(String pointCols) {
		Utilities.checkNotNullArgument(pointCols, "Point columns are null");

		try {
			CSVFormat format = CSVFormat.DEFAULT.withDelimiter(delimiter());
			quote().ifPresent(format::withQuote);
			CSVRecord rec = format.parse(new StringReader(pointCols))
									.getRecords()
									.get(0);
			if ( rec.size() != 3 ) {
				throw new IllegalArgumentException("invalid point column expr='" + pointCols + "'");
			}
			
			m_pointCols = Tuple.of("the_geom", rec.get(0), rec.get(1), rec.get(2));
			m_pointColsExpr = pointCols;
		}
		catch ( IOException ignored ) {
			throw new RuntimeException("fails to parse 'point_col' paramenter: " + m_pointColsExpr
										+ ", cause=" + ignored);
		}
		
		return this;
	}
	
	private static final Pattern PATTERN = Pattern.compile("(\\S+)\\s*\\(\\s*(\\S+)\\s*\\)");
	private static Tuple<String,String> parseGeometryColumn(String str) {
		Matcher matcher = PATTERN.matcher(str.trim());
		if ( !matcher.find() ) {
			throw new IllegalArgumentException(String.format("invalid: '%s'", str));
		}
		
		return Tuple.of(matcher.group(1), matcher.group(2));
	}
	
	public FOption<Tuple4<String,String,String,String>> pointColumns() {
		return FOption.ofNullable(m_pointCols);
	}
	
	public boolean tiger() {
		return m_tiger;
	}

	@Option(names={"--tiger"}, description="use the tiger format")
	public CsvParameters tiger(boolean flag) {
		m_tiger = flag;
		return this;
	}
	
	public CsvParameters duplicate() {
		CsvParameters dupl = create();
		dupl.m_delim = m_delim;
		dupl.m_charset = m_charset;
		dupl.m_headerFirst = m_headerFirst;
		dupl.m_quote = m_quote;
		dupl.m_escape = m_escape;
		dupl.m_comment = m_comment;
		dupl.m_pointCols = m_pointCols;
		dupl.m_trimColumns = m_trimColumns;
		dupl.m_header = m_header;
		dupl.m_nullValue = m_nullValue;
		dupl.m_tiger = m_tiger;
		
		return dupl;
	}
	
	public StoreAsCsvOptions toStoreOptions() {
		StoreAsCsvOptions opts = StoreAsCsvOptions.DEFAULT(m_delim)
													.headerFirst(m_headerFirst)
													.charset(m_charset);
		opts = quote().transform(opts, (o,q) ->  o.quote(q));
		opts = escape().transform(opts, (o,esc) ->  o.quote(esc));
		
		return opts;
	}
	
	public CsvOptions toCsvOptions() {
		CsvOptions opts = CsvOptions.DEFAULT(m_delim)
									.headerFirst(m_headerFirst)
									.charset(m_charset);
		opts = quote().transform(opts, (o,q) ->  o.quote(q));
		opts = escape().transform(opts, (o,esc) ->  o.quote(esc));
		
		return opts;
	}
	
	@Override
	public String toString() {
		String headerFirst = m_headerFirst ? ",HF" : ""; 
		String nullString = nullValue().map(v -> String.format(", null=\"%s\"", v))
										.getOrElse("");
		String csStr = charset().toString().toLowerCase();
		csStr = !csStr.equals("utf-8") ? String.format(",%s", csStr) : "";
		String ptStr = pointColumns().map(xy -> String.format(", %s(%s)(%s,%s)", xy._1, xy._2, xy._3, xy._4))
									.getOrElse("");
		return String.format("'%s'%s%s%s%s",
								m_delim, headerFirst, ptStr, csStr, nullString);
	}
}
