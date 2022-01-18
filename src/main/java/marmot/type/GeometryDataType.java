package marmot.type;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Objects;

import javax.annotation.Nullable;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.InputStreamInStream;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKBWriter;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.io.WKTWriter;

import utils.Utilities;
import utils.func.FOption;


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
	
	protected GeometryDataType(TypeClass tc, Class<?> instClass, @Nullable String srid) {
		super(encodeTypeId(tc, srid), tc, instClass); 
		
		m_srid = srid;
		m_displayName = encodeTypeName(tc, srid);
	}
	
	@Nullable
	public String srid() {
		return m_srid;
	}
	
	public static String getEpsgCode(String srid) {
		return FOption.ofNullable(srid)
						.map(s -> s.substring(5))
						.getOrElse("0");
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
		Utilities.checkNotNullArgument(geom, "input Geometry is null");
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
		return typeClass().equals(other.typeClass()) && Objects.equals(m_srid, other.m_srid);
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
		Utilities.checkNotNullArgument(geom, "input Geometry is null");
		
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
		Utilities.checkNotNullArgument(geom, "input Geometry is null");
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
		return String.format("%d(%s)", tc.get(), getEpsgCode(srid));
	}
	
	private static String encodeTypeName(TypeClass tc, String srid) {
		return String.format("%s(%s)", tc.name(), getEpsgCode(srid));
	}
}
