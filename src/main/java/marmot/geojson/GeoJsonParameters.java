package marmot.geojson;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import javax.annotation.Nullable;

import picocli.CommandLine.Option;
import utils.Utilities;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GeoJsonParameters {
	private static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;
	
	private Charset m_charset = DEFAULT_CHARSET;
	private @Nullable String m_gjsonSrid = null;
	
	public static GeoJsonParameters create() {
		return new GeoJsonParameters();
	}
	
	public Charset charset() {
		return m_charset;
	}

	@Option(names={"-charset"}, paramLabel="charset",
			description={"Character encoding of the target geojson file"})
	public GeoJsonParameters charset(String charset) {
		Utilities.checkNotNullArgument(charset, "charset");
		
		m_charset = Charset.forName(charset);
		return this;
	}
	
	public GeoJsonParameters charset(Charset charset) {
		Utilities.checkNotNullArgument(charset, "charset");
		
		m_charset = charset;
		return this;
	}

	@Option(names= {"-srid"}, paramLabel="EPSG-code", description="SRID for GeoJson file")
	public GeoJsonParameters geoJsonSrid(String srid) {
		m_gjsonSrid = srid;
		return this;
	}
	
	public FOption<String> geoJsonSrid() {
		return FOption.ofNullable(m_gjsonSrid);
	}
	
	@Override
	public String toString() {
		String srcSrid = geoJsonSrid().map(s -> String.format(", src_srid=%s", s))
										.getOrElse("");
		return String.format("charset=%s%s", charset(), srcSrid);
	}
}