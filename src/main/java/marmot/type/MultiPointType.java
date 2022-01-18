package marmot.type;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.MultiPoint;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MultiPointType extends GeometryDataType {
	private static final long serialVersionUID = 1L;
	
	public final static MultiPoint EMPTY = new GeometryFactory().createMultiPoint((Coordinate[])null);
	
	public static MultiPointType of(String srid) {
		return new MultiPointType(srid);
	}
	
	private MultiPointType(String srid) {
		super(TypeClass.MULTI_POINT, MultiPoint.class, srid);
	}

	@Override
	public MultiPointType duplicate(String srid) {
		return MultiPointType.of(srid);
	}

	@Override
	public GeometryDataType pluralType() {
		return this;
	}

	@Override
	public MultiPoint newInstance() {
		return EMPTY;
	}
}
