package marmot.stream.geo;

import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GeomOpOptions {
	public static final GeomOpOptions DEFAULT = new GeomOpOptions(FOption.empty(), FOption.empty());
	
	private FOption<String> m_outColumn = FOption.empty();
	private FOption<Boolean> m_throwOpError = FOption.empty();
	
	private GeomOpOptions(FOption<String> outputColumns, FOption<Boolean> throwOpError) {
		m_outColumn = outputColumns;
		m_throwOpError = throwOpError;
	}
	
	public static GeomOpOptions OUTPUT(String outCol) {
		return new GeomOpOptions(FOption.of(outCol), FOption.empty());
	}
	
	public FOption<String> outputColumn() {
		return m_outColumn;
	}
	
	public GeomOpOptions outputColumn(String outCol) {
		return new GeomOpOptions(FOption.of(outCol), m_throwOpError);
	}
	
	public FOption<Boolean> throwOpError() {
		return m_throwOpError;
	}
	
	public GeomOpOptions throwOpError(boolean flag) {
		return new GeomOpOptions(m_outColumn, FOption.of(flag));
	}
}
