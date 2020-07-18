package marmot.shp;

import java.nio.charset.Charset;

import picocli.CommandLine.Option;
import utils.UnitUtils;
import utils.Utilities;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ExportShapefileParameters {
	private ShapefileParameters m_shpParams = ShapefileParameters.create();
	private String m_typeName = null;
	private long m_splitSize = -1;
	
	public static ExportShapefileParameters create() {
		return new ExportShapefileParameters();
	}
	
	public Charset charset() {
		return m_shpParams.charset();
	}

	@Option(names={"-charset"}, paramLabel="charset",
			description={"Character encoding of the target shapefile file"})
	public ExportShapefileParameters charset(String charset) {
		m_shpParams = m_shpParams.charset(charset);
		return this;
	}
	
	public ExportShapefileParameters charset(Charset charset) {
		m_shpParams = m_shpParams.charset(charset);
		return this;
	}
	
	@Option(names= {"-srid"}, paramLabel="EPSG-code", description="shapefile SRID")
	public ExportShapefileParameters shpSrid(String srid) {
		m_shpParams = m_shpParams.srid(srid);
		return this;
	}
	
	public FOption<String> shpSrid() {
		return m_shpParams.srid();
	}
	
	public ExportShapefileParameters typeName(String name) {
		m_typeName = name;
		return this;
	}
	
	public FOption<String> typeName() {
		return FOption.ofNullable(m_typeName);
	}

	public FOption<Long> splitSize() {
		return FOption.gtz(m_splitSize);
	}

	@Option(names= {"-split_size"}, paramLabel="bytes",
			description="Shapefile split size string (eg. '128mb')")
	public ExportShapefileParameters splitSize(String splitSize) {
		return splitSize(UnitUtils.parseByteSize(splitSize));
	}
	
	public ExportShapefileParameters splitSize(long splitSize) {
		Utilities.checkArgument(splitSize > 0, "splitSize > 0");
		
		m_splitSize = splitSize;
		return this;
	}
}