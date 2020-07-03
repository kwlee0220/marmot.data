package marmot.type;

import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.MultiPolygon;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MultiPolygonType extends GeometryDataType {
	private static final long serialVersionUID = 1L;
	
	public final static MultiPolygon EMPTY = new GeometryFactory().createMultiPolygon(null);
	
	public static MultiPolygonType of(String srid) {
		return new MultiPolygonType(srid);
	}
	
	private MultiPolygonType(String srid) {
		super(TypeClass.MULTI_POLYGON, MultiPolygon.class, srid);
	}

	@Override
	public MultiPolygonType duplicate(String srid) {
		return MultiPolygonType.of(srid);
	}

	@Override
	public GeometryDataType pluralType() {
		return this;
	}

	@Override
	public MultiPolygon newInstance() {
		return EMPTY;
	}
}
