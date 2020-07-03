package marmot.type;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;

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
}
