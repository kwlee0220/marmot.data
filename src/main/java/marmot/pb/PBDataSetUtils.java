package marmot.pb;

import com.google.protobuf.ByteString;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;

import marmot.type.GeometryCollectionType;
import marmot.type.GeometryDataType;
import marmot.type.GeometryType;
import marmot.type.LineStringType;
import marmot.type.MultiLineStringType;
import marmot.type.MultiPointType;
import marmot.type.MultiPolygonType;
import marmot.type.PointType;
import marmot.type.PolygonType;
import marmot.type.TypeClass;
import proto.CoordinateProto;
import proto.EnvelopeProto;
import proto.GeometryProto;
import proto.TypeCodeProto;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PBDataSetUtils {
	private PBDataSetUtils() {
		throw new AssertionError("Should not be called here: class=" + getClass());
	}
	
	public static Coordinate fromProto(CoordinateProto proto) {
		return new Coordinate(proto.getX(), proto.getY());
	}
	
	public static CoordinateProto toProto(Coordinate coord) {
		return CoordinateProto.newBuilder()
								.setX(coord.x)
								.setY(coord.y)
								.build();
	}
	
	public static Envelope fromProto(EnvelopeProto proto) {
		return new Envelope(fromProto(proto.getTl()), fromProto(proto.getBr()));
	}
	
	public static EnvelopeProto toProto(Envelope envl) {
		return EnvelopeProto.newBuilder()
							.setTl(CoordinateProto.newBuilder()
												.setX(envl.getMinX())
												.setY(envl.getMinY())
												.build())
							.setBr(CoordinateProto.newBuilder()
												.setX(envl.getMaxX())
												.setY(envl.getMaxY())
												.build())
							.build();
	}

	public static TypeCodeProto toProto(TypeClass tc) {
		return TypeCodeProto.forNumber(tc.get());
	}
	public static TypeClass fromProto(TypeCodeProto proto) {
		return TypeClass.fromCode(proto.getNumber());
	}
	
	public static Geometry fromProto(GeometryProto proto) {
		switch ( proto.getEitherCase() ) {
			case POINT:
				return PointType.toPoint(fromProto(proto.getPoint()));
			case WKB:
				return GeometryDataType.fromWkb(proto.getWkb().toByteArray());
			case EMPTY_GEOM_TC:
				switch ( fromProto(proto.getEmptyGeomTc()) ) {
					case POINT: return PointType.EMPTY;
					case MULTI_POINT: return MultiPointType.EMPTY;
					case LINESTRING: return LineStringType.EMPTY;
					case MULTI_LINESTRING: return MultiLineStringType.EMPTY;
					case POLYGON: return PolygonType.EMPTY;
					case MULTI_POLYGON: return MultiPolygonType.EMPTY;
					case GEOM_COLLECTION: return GeometryCollectionType.EMPTY;
					case GEOMETRY: return GeometryType.EMPTY;
					default: throw new IllegalArgumentException("unexpected Geometry type: code=" + fromProto(proto.getEmptyGeomTc()));
				}
			case EITHER_NOT_SET:
				return null;
			default:
				throw new AssertionError();
		}
	}

	private static final GeometryProto NULL_GEOM = GeometryProto.newBuilder().build();
	public static GeometryProto toProto(Geometry geom) {
		if ( geom == null ) {
			return NULL_GEOM;
		}
		else if ( geom.isEmpty() ) {
			TypeClass tc = GeometryDataType.fromGeometry(geom);
			TypeCodeProto tcProto = toProto(tc);
			return GeometryProto.newBuilder().setEmptyGeomTc(tcProto).build();
		}
		else if ( geom instanceof Point ) {
			Point pt = (Point)geom;
			return GeometryProto.newBuilder().setPoint(toProto(pt.getCoordinate())).build();
		}
		else {
			ByteString wkb = ByteString.copyFrom(GeometryDataType.toWkb(geom));
			return GeometryProto.newBuilder().setWkb(wkb).build();
		}
	}
}
