package marmot.shp;

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
public class ShapefileParameters {
	private static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;
	
	private @Nullable Charset m_charset = DEFAULT_CHARSET;
	private @Nullable String m_shpSrid = null;
	
	public static ShapefileParameters create() {
		return new ShapefileParameters();
	}
	
	public Charset charset() {
		return m_charset;
	}

	@Option(names={"--charset"}, paramLabel="charset",
			description={"Character encoding of the target shapefile file"})
	public ShapefileParameters charset(String charset) {
		Utilities.checkNotNullArgument(charset);
		
		m_charset = Charset.forName(charset);
		return this;
	}
	
	public ShapefileParameters charset(Charset charset) {
		Utilities.checkNotNullArgument(charset);
		
		m_charset = charset;
		return this;
	}
	
	@Option(names= {"--srid"}, paramLabel="EPSG-code", description="shapefile SRID")
	public ShapefileParameters srid(String srid) {
		m_shpSrid = srid;
		return this;
	}
	
	public FOption<String> srid() {
		return FOption.ofNullable(m_shpSrid);
	}
	
	@Override
	public String toString() {
		String srcSrid = srid().map(s -> String.format(", srid=%s", s))
								.getOrElse("");
		return String.format("charset=%s%s", charset(), srcSrid);
	}
}