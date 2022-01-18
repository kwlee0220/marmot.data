package marmot.type;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GeometryCollectionType extends GeometryDataType {
	private static final long serialVersionUID = 1L;
	
	public final static GeometryCollection EMPTY = new GeometryFactory().createGeometryCollection(new Geometry[0]);
	
	public static GeometryCollectionType of(String srid) {
		return new GeometryCollectionType(srid);
	}
	
	private GeometryCollectionType(String srid) {
		super(TypeClass.GEOM_COLLECTION, GeometryCollection.class, srid);
	}

	@Override
	public GeometryCollectionType duplicate(String srid) {
		return GeometryCollectionType.of(srid);
	}

	@Override
	public GeometryDataType pluralType() {
		return this;
	}

	@Override
	public GeometryCollection newInstance() {
		return EMPTY;
	}
}
