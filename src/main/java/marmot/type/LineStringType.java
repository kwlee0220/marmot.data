package marmot.type;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class LineStringType extends GeometryDataType {
	private static final long serialVersionUID = 1L;
	
	public final static LineString EMPTY = new GeometryFactory().createLineString(new Coordinate[0]);
	
	public static LineStringType of(String srid) {
		return new LineStringType(srid);
	}
	
	private LineStringType(String srid) {
		super(TypeClass.LINESTRING, LineString.class, srid);
	}

	@Override
	public LineStringType duplicate(String srid) {
		return LineStringType.of(srid);
	}

	@Override
	public MultiLineStringType pluralType() {
		return MultiLineStringType.of(srid());
	}

	@Override
	public LineString newInstance() {
		return EMPTY;
	}
}
