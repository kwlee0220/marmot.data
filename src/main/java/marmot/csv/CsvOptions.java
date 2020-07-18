package marmot.csv;

import java.nio.charset.Charset;

import utils.Utilities;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class CsvOptions /*implements PBSerializable<CsvOptionsProto>*/ {
	private final char m_delim;
	private final FOption<Character> m_quote;
	private final FOption<Character> m_escape;
	private final FOption<Charset> m_charset;
	private final FOption<Boolean> m_headerFirst;
	
	private CsvOptions(char delim, FOption<Character> quote, FOption<Character> escape,
						FOption<Charset> charset, FOption<Boolean> headerFirst) {
		m_delim = delim;
		m_quote = quote;
		m_escape = escape;
		m_charset = charset;
		m_headerFirst = headerFirst;
	}
	
	public static CsvOptions DEFAULT() {
		return new CsvOptions(',', FOption.empty(), FOption.empty(), FOption.empty(),
							FOption.empty());
	}
	
	public static CsvOptions DEFAULT(char delim) {
		return new CsvOptions(delim, FOption.empty(), FOption.empty(), FOption.empty(),
							FOption.empty());
	}
	
	public static CsvOptions DEFAULT(char delim, char quote) {
		return new CsvOptions(delim, FOption.of(quote), FOption.empty(), FOption.empty(),
							FOption.empty());
	}
	
	public static CsvOptions DEFAULT(char delim, char quote, char escape) {
		return new CsvOptions(delim, FOption.of(quote), FOption.of(escape),
							FOption.empty(), FOption.empty());
	}
	
	public char delimiter() {
		return m_delim;
	}

	public CsvOptions delimiter(char delim) {
		return new CsvOptions(delim, m_quote, m_escape, m_charset, m_headerFirst);
	}
	
	public FOption<Character> quote() {
		return m_quote;
	}

	public CsvOptions quote(char quote) {
		return new CsvOptions(m_delim, FOption.of(quote), m_escape, m_charset, m_headerFirst);
	}
	
	public FOption<Character> escape() {
		return m_escape;
	}
	
	public CsvOptions escape(char escape) {
		return new CsvOptions(m_delim, m_quote, FOption.of(escape), m_charset, m_headerFirst);
	}
	
	public FOption<Charset> charset() {
		return m_charset;
	}

	public CsvOptions charset(String charset) {
		return charset(Charset.forName(charset));
	}
	
	public CsvOptions charset(Charset charset) {
		Utilities.checkNotNullArgument(charset, "Charset is null");
		
		return new CsvOptions(m_delim, m_quote, m_escape, FOption.of(charset), m_headerFirst);
	}
	
	public FOption<Boolean> headerFirst() {
		return m_headerFirst;
	}
	
	public CsvOptions headerFirst(Boolean flag) {
		return new CsvOptions(m_delim, m_quote, m_escape, m_charset, FOption.ofNullable(flag));
	}
	
	@Override
	public String toString() {
		String csStr = !charset().toString().equalsIgnoreCase("utf-8")
						? String.format(", %s", charset().toString()) : "";
		return String.format("delim='%s'%s", m_delim, csStr);
	}

/*
	public static CsvOptions fromProto(CsvOptionsProto proto) {
		CsvOptions opts = CsvOptions.DEFAULT(proto.getDelimiter().charAt(0));
		
		switch ( proto.getOptionalQuoteCase() ) {
			case QUOTE:
				opts = opts.quote(proto.getQuote().charAt(0));
				break;
			default:
		}
		switch ( proto.getOptionalEscapeCase() ) {
			case ESCAPE:
				opts = opts.escape(proto.getEscape().charAt(0));
				break;
			default:
		}
		switch ( proto.getOptionalCharsetCase() ) {
			case CHARSET:
				opts = opts.charset(proto.getCharset());
				break;
			default:
		}
		switch ( proto.getOptionalHeaderFirstCase() ) {
			case HEADER_FIRST:
				opts = opts.headerFirst(proto.getHeaderFirst());
				break;
			default:
		}
		
		
		return opts;
	}

	@Override
	public CsvOptionsProto toProto() {
		CsvOptionsProto.Builder builder = CsvOptionsProto.newBuilder()
														.setDelimiter(""+m_delim);
		
		m_quote.map(c -> c.toString()).ifPresent(builder::setQuote);
		m_escape.map(c -> c.toString()).ifPresent(builder::setEscape);
		m_charset.map(Charset::name).ifPresent(builder::setCharset);
		m_headerFirst.ifPresent(builder::setHeaderFirst);
		
		return builder.build();
	}
*/
}
