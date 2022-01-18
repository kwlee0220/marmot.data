package marmot.type;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PointType extends GeometryDataType {
	private static final long serialVersionUID = 1L;
	
	public final static Point EMPTY = new GeometryFactory().createPoint((Coordinate)null);
	
	public static PointType of(String srid) {
		return new PointType(srid);
	}
	
	private PointType(String srid) {
		super(TypeClass.POINT, Point.class, srid);
	}

	@Override
	public PointType duplicate(String srid) {
		return PointType.of(srid);
	}

	@Override
	public MultiPointType pluralType() {
		return MultiPointType.of(srid());
	}

	@Override
	public Point newInstance() {
		return EMPTY;
	}
	
	public static Point toPoint(Coordinate coord) {
		return GEOM_FACT.createPoint(coord);
	}
	
	public static Point toPoint(double x, double y) {
		return toPoint(new Coordinate(x, y));
	}
}
