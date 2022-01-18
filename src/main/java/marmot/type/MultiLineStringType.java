package marmot.type;

import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.MultiLineString;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MultiLineStringType extends GeometryDataType {
	private static final long serialVersionUID = 1L;
	
	public final static MultiLineString EMPTY = new GeometryFactory().createMultiLineString(null);
	
	public static MultiLineStringType of(String srid) {
		return new MultiLineStringType(srid);
	}
	
	private MultiLineStringType(String srid) {
		super(TypeClass.MULTI_LINESTRING, MultiLineString.class, srid);
	}

	@Override
	public MultiLineStringType duplicate(String srid) {
		return MultiLineStringType.of(srid);
	}

	@Override
	public GeometryDataType pluralType() {
		return this;
	}

	@Override
	public MultiLineString newInstance() {
		return EMPTY;
	}
}
