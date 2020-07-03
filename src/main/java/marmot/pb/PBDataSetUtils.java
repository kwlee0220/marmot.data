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
	
/*
	public static final SerializedProto serialize(Object obj) {
		if ( obj instanceof PBSerializable ) {
			return ((PBSerializable<?>)obj).serialize();
		}
		else if ( obj instanceof Message ) {
			return PBUtils.serialize((Message)obj);
		}
		else if ( obj instanceof Serializable ) {
			return PBUtils.serializeJava((Serializable)obj);
		}
		else {
			throw new IllegalStateException("unable to serialize: " + obj);
		}
	}
	
	public static final SerializedProto serializeJava(Serializable obj) {
		try {
			JavaSerializedProto proto = JavaSerializedProto.newBuilder()
										.setSerialized(ByteString.copyFrom(IOUtils.serialize(obj)))
										.build();
			return SerializedProto.newBuilder()
									.setJava(proto)
									.build();
		}
		catch ( Exception e ) {
			throw new PBException("fails to serialize object: proto=" + obj, e);
		}
	}
	
	public static final SerializedProto serialize(Message proto) {
		ProtoBufSerializedProto serialized = ProtoBufSerializedProto.newBuilder()
										.setProtoClass(proto.getClass().getName())
										.setSerialized(proto.toByteString())
										.build();
		return SerializedProto.newBuilder()
								.setProtoBuf(serialized)
								.build();
	}
	
	public static final <T> T deserialize(SerializedProto proto) {
		switch ( proto.getMethodCase() ) {
			case PROTO_BUF:
				return deserialize(proto.getProtoBuf());
			case JAVA:
				return deserialize(proto.getJava());
			default:
				throw new AssertionError("unregistered serialization method: method="
										+ proto.getMethodCase());
		}
	}
	
	@SuppressWarnings("unchecked")
	public static <T> T deserialize(JavaSerializedProto proto) {
		try {
			return (T)IOUtils.deserialize(proto.getSerialized().toByteArray());
		}
		catch ( Exception e ) {
			Throwable cause = Throwables.unwrapThrowable(e);
			throw new PBException("fails to deserialize: proto=" + proto, cause);
		}
	}
	
	@SuppressWarnings("unchecked")
	public static final <T> T deserialize(ProtoBufSerializedProto proto) {
		try {
			ByteString serialized = proto.getSerialized();
			
			FOption<String> clsName = getOptionField(proto, "object_class");
			Class<?> protoCls = Class.forName(proto.getProtoClass());

			Method parseFrom = protoCls.getMethod("parseFrom", ByteString.class);
			Message optorProto = (Message)parseFrom.invoke(null, serialized);
			
			if ( clsName.isPresent() ) {
				Class<?> cls = Class.forName(clsName.get());
				Method fromProto = cls.getMethod("fromProto", protoCls);
				return (T)fromProto.invoke(null, optorProto);
			}
			else {
				return (T)ProtoBufActivator.activate(optorProto);
			}
		}
		catch ( Exception e ) {
			Throwable cause = Throwables.unwrapThrowable(e);
			throw new PBException("fails to deserialize: proto=" + proto + ", cause=" + cause, cause);
		}
	}
	
	public static Enum<?> getCase(Message proto, String field) {
		try {
			String partName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, field);
			Method getCase = proto.getClass().getMethod("get" + partName + "Case", new Class<?>[0]);
			return (Enum<?>)getCase.invoke(proto, new Object[0]);
		}
		catch ( Exception e ) {
			throw new PBException("fails to get the case " + field, e);
		}
	}

	@SuppressWarnings("unchecked")
	public static <T> FOption<T> getOptionField(Message proto, String field) {
		try {
			return FOption.ofNullable((T)KVFStream.from(proto.getAllFields())
												.filter(kv -> kv.key().getName().equals(field))
												.next()
												.map(kv -> kv.value())
												.getOrNull());
					}
		catch ( Exception e ) {
			throw new PBException("fails to get the field " + field, e);
		}
	}

	public static FOption<String> getStringOptionField(Message proto, String field) {
		return getOptionField(proto, field).cast(String.class);
	}

	public static FOption<Double> getDoubleOptionField(Message proto, String field) {
		return getOptionField(proto, field).cast(Double.class);
	}

	public static FOption<Long> geLongOptionField(Message proto, String field) {
		return getOptionField(proto, field).cast(Long.class);
	}

	public static FOption<Integer> getIntOptionField(Message proto, String field) {
		return getOptionField(proto, field).cast(Integer.class);
	}

	@SuppressWarnings("unchecked")
	public static <T> T getField(Message proto, String field) {
		try {
			return(T)KVFStream.from(proto.getAllFields())
									.filter(kv -> kv.key().getName().equals(field))
									.next()
									.map(kv -> kv.value())
									.getOrElseThrow(()
										-> new PBException("unknown field: name=" + field
																	+ ", msg=" + proto));
		}
		catch ( Exception e ) {
			throw new PBException("fails to get the field " + field, e);
		}
	}
	
	public static VoidResponse toVoidResponse() {
		return VOID_RESPONSE;
	}
	
	public static VoidResponse toVoidResponse(Throwable e) {
		return VoidResponse.newBuilder()
							.setError(toErrorProto(e))
							.build();
	}
	
	public static StringResponse toStringResponse(Throwable e) {
		return StringResponse.newBuilder()
							.setError(toErrorProto(e))
							.build();
	}
	
	public static StringResponse toStringResponse(String value) {
		return StringResponse.newBuilder()
							.setValue(value)
							.build();
	}
	
	public static String getValue(StringResponse resp) {
		switch ( resp.getEitherCase() ) {
			case VALUE:
				return resp.getValue();
			case ERROR:
				throw Throwables.toRuntimeException(PBUtils.toException(resp.getError()));
			default:
				throw new AssertionError();
		}
	}
	
	public static BoolResponse toBoolResponse(boolean value) {
		return BoolResponse.newBuilder()
							.setValue(value)
							.build();
	}
	public static BoolResponse toBoolResponse(Throwable e) {
		return BoolResponse.newBuilder()
							.setError(toErrorProto(e))
							.build();
	}
	public static LongResponse toLongResponse(Throwable e) {
		return LongResponse.newBuilder()
							.setError(toErrorProto(e))
							.build();
	}
	
	public static FloatResponse toFloatResponse(float value) {
		return FloatResponse.newBuilder()
							.setValue(value)
							.build();
	}
	
	public static FloatResponse toFloatResponse(Throwable e) {
		return FloatResponse.newBuilder()
							.setError(toErrorProto(e))
							.build();
	}
	
	public static DoubleResponse toDoubleResponse(double value) {
		return DoubleResponse.newBuilder()
							.setValue(value)
							.build();
	}
	
	public static DoubleResponse toDoubleResponse(Throwable e) {
		return DoubleResponse.newBuilder()
							.setError(toErrorProto(e))
							.build();
	}
	
	public static RecordResponse toRecordResponse(Record value) {
		return RecordResponse.newBuilder()
							.setRecord(value.toProto())
							.build();
	}
	
	public static RecordResponse toRecordResponse(Throwable e) {
		return RecordResponse.newBuilder()
							.setError(toErrorProto(e))
							.build();
	}

	public static <T extends Message> FStream<T> toFStream(Iterator<T> respIter) {
		if ( !respIter.hasNext() ) {
			// Iterator가 empty인 경우는 예외가 발생하지 않았고, 결과가 없는 경우를
			// 의미하기 때문에 empty FStream을 반환한다.
			return FStream.empty();
		}
		
		PeekingIterator<T> piter = Iterators.peekingIterator(respIter);
		T proto = piter.peek();
		FOption<ErrorProto> error = getOptionField(proto, "error");
		if ( error.isPresent() ) {
			throw Throwables.toRuntimeException(toException(error.get()));
		}
		
		return FStream.from(piter);
	}
	
	public static void handle(VoidResponse resp) {
		switch ( resp.getEitherCase() ) {
			case VOID:
				return;
			case ERROR:
				throw Throwables.toRuntimeException(toException(resp.getError()));
			default:
				throw new AssertionError();
		}
	}
	
	public static String handle(StringResponse resp) {
		switch ( resp.getEitherCase() ) {
			case VALUE:
				return resp.getValue();
			case ERROR:
				throw Throwables.toRuntimeException(toException(resp.getError()));
			default:
				throw new AssertionError();
		}
	}
	
	public static boolean handle(BoolResponse resp) {
		switch ( resp.getEitherCase() ) {
			case VALUE:
				return resp.getValue();
			case ERROR:
				throw Throwables.toRuntimeException(toException(resp.getError()));
			default:
				throw new AssertionError();
		}
	}
	
	public static long handle(LongResponse resp) {
		switch ( resp.getEitherCase() ) {
			case VALUE:
				return resp.getValue();
			case ERROR:
				throw Throwables.toRuntimeException(toException(resp.getError()));
			default:
				throw new AssertionError();
		}
	}
	
	public static float handle(FloatResponse resp) {
		switch ( resp.getEitherCase() ) {
			case VALUE:
				return resp.getValue();
			case ERROR:
				throw Throwables.toRuntimeException(toException(resp.getError()));
			default:
				throw new AssertionError();
		}
	}
	
	public static double handle(DoubleResponse resp) {
		switch ( resp.getEitherCase() ) {
			case VALUE:
				return resp.getValue();
			case ERROR:
				throw Throwables.toRuntimeException(toException(resp.getError()));
			default:
				throw new AssertionError();
		}
	}
	
	public static <X extends Throwable> void replyBoolean(CheckedSupplier<Boolean> supplier,
									StreamObserver<BoolResponse> response) {
		try {
			boolean done = supplier.get();
			response.onNext(BoolResponse.newBuilder()
										.setValue(done)
										.build());
		}
		catch ( Throwable e ) {
			response.onNext(BoolResponse.newBuilder()
										.setError(PBUtils.toErrorProto(e))
										.build());
		}
		response.onCompleted();
	}
	
	public static <X extends Throwable> void replyLong(CheckedSupplier<Long> supplier,
							StreamObserver<LongResponse> response) {
		try {
			long ret = supplier.get();
			response.onNext(LongResponse.newBuilder()
										.setValue(ret)
										.build());
		}
		catch ( Throwable e ) {
			response.onNext(LongResponse.newBuilder()
										.setError(PBUtils.toErrorProto(e))
										.build());
		}
		response.onCompleted();
	}
	
	public static <X extends Throwable> void replyString(CheckedSupplier<String> supplier,
									StreamObserver<StringResponse> response) {
		try {
			String ret = supplier.get();
			response.onNext(StringResponse.newBuilder()
										.setValue(ret)
										.build());
		}
		catch ( Throwable e ) {
			response.onNext(StringResponse.newBuilder()
										.setError(PBUtils.toErrorProto(e))
										.build());
		}
		response.onCompleted();
	}
	
	public static void replyVoid(CheckedRunnable runnable,
									StreamObserver<VoidResponse> response) {
		try {
			runnable.run();
			response.onNext(VoidResponse.newBuilder()
										.setVoid(VOID)
										.build());
		}
		catch ( Throwable e ) {
			response.onNext(VoidResponse.newBuilder()
										.setError(PBUtils.toErrorProto(e))
										.build());
		}
		response.onCompleted();
	}
	
	public static byte[] toDelimitedBytes(Message proto) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try {
			proto.writeTo(baos);
		}
		catch ( IOException e ) {
			throw new RuntimeException(e);
		}
		finally {
			IOUtils.closeQuietly(baos);
		}
		
		return baos.toByteArray();
	}

	public static boolean fromProto(BoolProto proto) {
		return proto.getValue();
	}
	
	public static BoolProto toProto(boolean value) {
		return BoolProto.newBuilder().setValue(value).build();
	}
	
	public static Size2i fromProto(Size2iProto proto) {
		return new Size2i(proto.getWidth(), proto.getHeight());
	}
	
	public static Size2iProto toProto(Size2i dim) {
		return Size2iProto.newBuilder()
									.setWidth(dim.getWidth())
									.setHeight(dim.getHeight())
									.build();
	}
	
	public static Size2d fromProto(Size2dProto proto) {
		return new Size2d(proto.getWidth(), proto.getHeight());
	}
	
	public static Size2dProto toProto(Size2d dim) {
		return Size2dProto.newBuilder()
						.setWidth(dim.getWidth())
						.setHeight(dim.getHeight())
						.build();
	}
	
	public static Interval fromProto(IntervalProto proto) {
		return Interval.between(proto.getStart(), proto.getEnd());
	}
	
	public static IntervalProto toProto(Interval intvl) {
		return IntervalProto.newBuilder()
							.setStart(intvl.getStartMillis())
							.setEnd(intvl.getEndMillis())
							.build();
	}
*/
	
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
