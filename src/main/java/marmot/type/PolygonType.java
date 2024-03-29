package marmot.type;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PolygonType extends GeometryDataType {
	private static final long serialVersionUID = 1L;
	
	public final static Polygon EMPTY = new GeometryFactory().createPolygon(new Coordinate[0]);
	
	public static PolygonType of(String srid) {
		return new PolygonType(srid);
	}
	
	private PolygonType(String srid) {
		super(TypeClass.POLYGON, Polygon.class, srid);
	}

	@Override
	public PolygonType duplicate(String srid) {
		return PolygonType.of(srid);
	}

	@Override
	public MultiPolygonType pluralType() {
		return MultiPolygonType.of(srid());
	}

	@Override
	public Polygon newInstance() {
		return EMPTY;
	}
}
