package marmot.type;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.MultiLineString;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.InputStreamInStream;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKBWriter;
import com.vividsolutions.jts.io.WKTReader;
import com.vividsolutions.jts.io.WKTWriter;

import utils.Utilities;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class GeometryDataType extends DataType {
	private static final long serialVersionUID = 1L;
	
	final static GeometryFactory GEOM_FACT = new GeometryFactory();
	final static Point EMPTY_POINT = GEOM_FACT.createPoint((Coordinate)null);
	
	private final String m_srid;
	private final String m_displayName;

	public abstract Geometry newInstance();
	public abstract GeometryDataType pluralType();
	public abstract GeometryDataType duplicate(String srid);
	
	protected GeometryDataType(TypeClass tc, Class<?> instClass, String srid) {
		super(encodeTypeId(tc, srid), tc, instClass); 
		
		m_srid = srid;
		m_displayName = encodeTypeName(tc, srid);
	}
	
	public String srid() {
		return m_srid;
	}

	@Override
	public String displayName() {
		return m_displayName;
	}
	
	@Override
	public Geometry parseInstance(String wkt) {
		return fromWkt(wkt);
	}
	
	@Override
	public String toInstanceString(Object geom) {
		Utilities.checkNotNullArgument(geom, "input Geometryis null");
		Utilities.checkArgument(geom instanceof Geometry, "input is not Geometry");
		
		return toWkt((Geometry)geom);
	}

	@Override
	public void serialize(Object obj, ObjectOutputStream oos) throws IOException {
		if ( obj instanceof Geometry ) {
			byte[] wkb = toWkb(((Geometry)obj));
			oos.writeInt(wkb.length);
			oos.write(wkb);
		}
		else {
			throw new IOException("input stream corrupted: not " + getClass());
		}
	}

	@Override
	public Object deserialize(ObjectInputStream ois) throws IOException {
		int nbytes = ois.readInt();
		byte[] wkb = new byte[nbytes];
		ois.readFully(wkb);
		
		return fromWkb(wkb);
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		else if ( obj == null || obj.getClass() != getClass() ) {
			return false;
		}
		
		GeometryDataType other = (GeometryDataType)obj;
		return typeClass().equals(other.typeClass()) && m_srid.equals(other.m_srid);
	}
	
	public static Geometry fromWkt(String wkt) {
		Utilities.checkNotNullArgument(wkt, "input WKT is null");

		if ( wkt == null ) {
			return null;
		}
		else if ( wkt.length() == 0 ) {
			return GeometryType.EMPTY;
		}
		else {
			try {
				return new WKTReader(GEOM_FACT).read(wkt);
			}
			catch ( ParseException e ) {
				throw new IllegalArgumentException("invalid WKT: wkt=" + wkt);
			}
		}
	}
	
	public static String toWkt(Geometry geom) {
		Utilities.checkNotNullArgument(geom, "input Geometryis null");
		
		return (geom != null) ? new WKTWriter().write((Geometry)geom) : null;
	}
	
	public static Geometry fromWkb(InputStream is) throws IOException {
		if ( is == null ) {
			return null;
		}
		else {
			try {
				return new WKBReader(GEOM_FACT).read(new InputStreamInStream(is));
			}
			catch ( ParseException e ) {
				throw new IllegalArgumentException("invalid WKB");
			}
		}
	}
	
	public static Geometry fromWkb(byte[] wkb) {
		Utilities.checkNotNullArgument(wkb, "input WKB is null");

		if ( wkb == null ) {
			return null;
		}
		else if ( wkb.length == 0 ) {
			return GeometryType.EMPTY;
		}
		else {
			try {
				return new WKBReader(GEOM_FACT).read(wkb);
			}
			catch ( ParseException e ) {
				throw new IllegalArgumentException("invalid WKB");
			}
		}
	}
	
	public static byte[] toWkb(Geometry geom) {
		Utilities.checkNotNullArgument(geom, "input Geometryis null");
		Utilities.checkArgument(geom instanceof Geometry, "input is not Geometry");
		
		return (geom != null) ? new WKBWriter().write(geom) : null;
	}
	
	public static TypeClass fromGeometry(Geometry geom) {
		if ( geom instanceof Point ) {
			return TypeClass.POINT;
		}
		else if ( geom instanceof Polygon ) {
			return TypeClass.POLYGON;
		}
		else if ( geom instanceof MultiPolygon ) {
			return TypeClass.MULTI_POLYGON;
		}
		else if ( geom instanceof LineString ) {
			return TypeClass.LINESTRING;
		}
		else if ( geom instanceof MultiLineString ) {
			return TypeClass.MULTI_LINESTRING;
		}
		else if ( geom instanceof MultiPoint ) {
			return TypeClass.MULTI_POINT;
		}
		else if ( geom instanceof GeometryCollection ) {
			return TypeClass.GEOM_COLLECTION;
		}
		else {
			throw new AssertionError();
		}
	}
	
	private static String encodeTypeId(TypeClass tc, String srid) {
		if ( srid.startsWith("EPSG:") ) {
			srid = srid.substring(5);
		}
		
		return String.format("%d(%s)", tc.get(), srid);
	}
	
	private static String encodeTypeName(TypeClass tc, String srid) {
		if ( srid.startsWith("EPSG:") ) {
			srid = srid.substring(5);
		}
		
		return String.format("%s(%s)", tc.name(), srid);
	}
}
