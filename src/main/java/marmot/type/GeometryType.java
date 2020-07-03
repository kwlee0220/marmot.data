package marmot.type;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GeometryType extends GeometryDataType {
	private static final long serialVersionUID = 1L;
	
	public final static Geometry EMPTY = new GeometryFactory().createGeometry(PointType.EMPTY);
	
	public static GeometryType of(String srid) {
		return new GeometryType(srid);
	}
	
	private GeometryType(String srid) {
		super(TypeClass.GEOMETRY, Geometry.class, srid);
	}

	@Override
	public GeometryType duplicate(String srid) {
		return GeometryType.of(srid);
	}

	@Override
	public GeometryDataType pluralType() {
		return this;
	}

	@Override
	public Geometry newInstance() {
		return EMPTY;
	}
}
