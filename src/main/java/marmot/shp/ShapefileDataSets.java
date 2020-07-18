package marmot.shp;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.geotools.data.collection.ListFeatureCollection;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.feature.FeatureIterator;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordStream;
import marmot.dataset.DataSetException;
import marmot.type.DataType;
import marmot.type.GeometryDataType;
import utils.Utilities;
import utils.geo.util.CRSUtils;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ShapefileDataSets {
	private static final Logger s_logger = LoggerFactory.getLogger(ShapefileDataSets.class);
	
	private ShapefileDataSets() {
		throw new AssertionError("Should not be called here: class=" + getClass());
	}
	
	public static SimpleFeatureType toSimpleFeatureType(String sfTypeName, String srid,
														RecordSchema schema) {
		Utilities.checkNotNullArgument(sfTypeName);
		Utilities.checkNotNullArgument(srid);
		Utilities.checkNotNullArgument(schema);
		
		SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
		builder.setName(sfTypeName);
		builder.setSRS(srid);
		
		Map<String,Integer> abbrs = Maps.newHashMap();
		schema.streamColumns()
				.forEach(col -> {
					String colName = col.name();
					if ( colName.length() > 10 ) {
						colName = colName.substring(0, 9);
						int seqno = abbrs.getOrDefault(colName, 0);
						abbrs.put(colName, (seqno+1));
						colName += (""+seqno);
						
						s_logger.warn(String.format("truncate too long field name: %s->%s",
													col.name(), colName));
					}
					
					switch ( col.type().typeClass() ) {
						case STRING:
							builder.nillable(true).add(colName, String.class);
							break;
						case DATE:
						case DATETIME:
							builder.add(colName, Date.class);
							break;
						default:
							builder.add(colName, col.type().instanceClass());
							break;
					}
				});
		return builder.buildFeatureType();
	}
	
	/**
	 * SimpleFeatureType로부터 RecordSchema 객체를 생성한다.
	 * 
	 * @param sfType	SimpleFeatureType 타입
	 * @return	RecordSchema
	 */
	public static RecordSchema toRecordSchema(SimpleFeatureType sfType) {
		Utilities.checkNotNullArgument(sfType);
		
		RecordSchema.Builder builder;
		try {
			CoordinateReferenceSystem crs = sfType.getCoordinateReferenceSystem();
			String srid = (crs != null) ? CRSUtils.toEPSG(crs) : null;
			
			builder = RecordSchema.builder();
			for ( AttributeDescriptor desc: sfType.getAttributeDescriptors() ) {
				Class<?> instCls = desc.getType().getBinding();
				DataType attrType = DataType.fromInstanceClass(instCls);
				if ( attrType.isGeometryType() ) {
					attrType = ((GeometryDataType)attrType).duplicate(srid);
				}
				builder.addColumn(desc.getLocalName(), attrType);
			}
		}
		catch ( FactoryException e ) {
			throw new DataSetException("fails to load CRS", e);
		}
		
		return builder.build();
	}
	
	public static SimpleFeatureRecordStream toRecordStream(SimpleFeatureType sfType,
															FeatureIterator<SimpleFeature> iter) {
		Utilities.checkNotNullArgument(sfType);
		Utilities.checkNotNullArgument(iter);

		return new SimpleFeatureRecordStream(sfType, iter);
	}
	
	public static SimpleFeatureRecordStream toRecordStream(FeatureIterator<SimpleFeature> iter) {
		Utilities.checkNotNullArgument(iter);
		Utilities.checkArgument(iter.hasNext(), "FeatureIterator is empty");

		return new SimpleFeatureRecordStream(iter);
	}
	
	public static SimpleFeatureRecordStream toRecordStream(SimpleFeatureCollection sfColl) {
		Utilities.checkNotNullArgument(sfColl);
		
		return toRecordStream(sfColl.getSchema(), sfColl.features());
	}
	
	public static SimpleFeatureRecordStream toRecordStream(SimpleFeatureSource sfSrc)
		throws IOException {
		Utilities.checkNotNullArgument(sfSrc);
		
		return toRecordStream(sfSrc.getFeatures());
	}
	
	public static List<SimpleFeature> toFeatureList(SimpleFeatureType sfType, RecordStream rset) {
		Utilities.checkNotNullArgument(sfType);
		Utilities.checkNotNullArgument(rset);
		
		List<SimpleFeature> features = Lists.newArrayList();
		SimpleFeatureBuilder builder = new SimpleFeatureBuilder(sfType);
		Record rec;
		while ( (rec = rset.nextCopy()) != null ) {
			SimpleFeature feature;
			if ( rec instanceof SimpleFeatureRecord ) {
				feature = ((SimpleFeatureRecord)rec).getSimpleFeature();
			}
			else {
				feature = builder.buildFeature(null, rec.getAll());
			}
			features.add(feature);
		}
		
		return features;
	}
	
	public static List<SimpleFeature> toFeatureList(SimpleFeatureType sfType,
													Iterable<Record> records) {
		Utilities.checkNotNullArgument(sfType);
		Utilities.checkNotNullArgument(records);
		
		SimpleFeatureBuilder builder = new SimpleFeatureBuilder(sfType);
		return FStream.from(records).map(r -> builder.buildFeature(null, r.getAll())).toList();
	}
	
	public static SimpleFeatureCollection toFeatureCollection(SimpleFeatureType sfType,
																Iterable<Record> records) {
		Utilities.checkNotNullArgument(sfType);
		Utilities.checkNotNullArgument(records);
		
		return new ListFeatureCollection(sfType, toFeatureList(sfType, records));
	}
}
