package marmot.remote.client;


import java.io.InputStream;

import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import marmot.DataSetInfo;
import marmot.DataSetNotFoundException;
import proto.dataset.DataSetInfoResponse;
import proto.dataset.DataSetServiceGrpc;
import proto.dataset.DataSetServiceGrpc.DataSetServiceBlockingStub;
import proto.dataset.DataSetServiceGrpc.DataSetServiceStub;
import proto.stream.UpMessage;
import utils.Throwables;
import utils.grpc.PBUtils;
import utils.grpc.stream.client.StreamDownloadReceiver;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GrpcDataSetServiceProxy {
	private final DataSetServiceBlockingStub m_blockStub;
	private final DataSetServiceStub m_stub;

	public GrpcDataSetServiceProxy(ManagedChannel channel) {
		m_stub = DataSetServiceGrpc.newStub(channel);
		m_blockStub = DataSetServiceGrpc.newBlockingStub(channel);
	}
	
	public GrpcDataSetProxy getDataSet(String id) {
		DataSetInfoResponse resp = m_blockStub.getDataSetInfo(PBUtils.STRING(id));
		return toDataSet(id, resp);
	}
	
	public InputStream readDataSet(String id) throws DataSetNotFoundException {
		StreamDownloadReceiver downloader = new StreamDownloadReceiver();

		// start download by sending 'stream-download' request
		StreamObserver<UpMessage> outChannel = m_stub.readDataSet(downloader);
		downloader.start(PBUtils.BYTE_STRING(id), outChannel);
		
		return downloader.getDownloadStream();
	}
	
//	public DataSet createDataSet(String dsId, RecordSchema schema, CreateDataSetOptions opts)
//			throws DataSetExistsException {
//		CreateDataSetRequest proto  = CreateDataSetRequest.newBuilder()
//															.setId(dsId)
//															.setRecordSchema(schema.toProto())
//															.setOptions(opts.toProto())
//															.build();
//		return toDataSet(m_dsBlockingStub.createDataSet(proto));
//	}
//	
//	public DataSet createDataSet(String dsId, Plan plan, StoreDataSetOptions opts)
//		throws DataSetExistsException {
//		CreateDataSetRequest proto = CreateDataSetRequest.newBuilder()
//														.setId(dsId)
//														.setOptions(opts.toCreateOptions().toProto())
//														.build();
//		return toDataSet(m_dsBlockingStub.createDataSet(proto));
//	}
//
//	public DataSet bindExternalDataSet(String dsId, String srcPath, DataSetType type,
//										BindDataSetOptions opts) {
//		DataSetTypeProto dsTypeProto = DataSetTypeProto.valueOf(type.id());
//		BindDataSetRequest.Builder builder = BindDataSetRequest.newBuilder()
//													.setDataset(dsId)
//													.setFilePath(srcPath)
//													.setType(dsTypeProto)
//													.setOptions(opts.toProto());
//		BindDataSetRequest req = builder.build();
//		
//		return toDataSet(m_dsBlockingStub.bindDataSet(req));
//	}
//
//	public DataSet buildDataSet(String dsId, String path, String infoPath,
//									BindDataSetOptions opts) {
//		BuildDataSetRequest.Builder builder = BuildDataSetRequest.newBuilder()
//													.setDataset(dsId)
//													.setFilePath(path)
//													.setInfoFilePath(infoPath)
//													.setOptions(opts.toProto());
//		BuildDataSetRequest req = builder.build();
//		
//		return toDataSet(m_dsBlockingStub.buildDataSet(req));
//	}

//	public DataSet getDataSetOrNull(String id) {
//		DataSetInfoResponse resp = m_dsBlockingStub.getDataSetInfo(PBUtils.toStringProto(id));
//		try {
//			return toDataSet(resp);
//		}
//		catch ( DataSetNotFoundException e ) {
//			return null;
//		}
//	}
//
//	public List<DataSet> getDataSetAll() {
//		return FStream.from(m_dsBlockingStub.getDataSetInfoAll(PBUtils.VOID))
//						.map(this::toDataSet)
//						.cast(DataSet.class)
//						.toList();
//	}
//
//	public List<DataSet> getDataSetAllInDir(String folder, boolean recursive) {
//		DirectoryTraverseRequest req = DirectoryTraverseRequest.newBuilder()
//													.setDirectory(folder)
//													.setRecursive(recursive)
//													.build();
//		return PBUtils.toFStream(m_dsBlockingStub.getDataSetInfoAllInDir(req))
//						.map(this::toDataSet)
//						.cast(DataSet.class)
//						.toList();
//	}
//	
//	public DataSet updateGeometryColumnInfo(String dsId, FOption<GeometryColumnInfo> info) {
//		UpdateGeometryColumnInfoRequest.Builder builder
//												= UpdateGeometryColumnInfoRequest.newBuilder()
//																				.setId(dsId);
//		info.map(GeometryColumnInfo::toProto)
//			.ifPresent(builder::setGcInfo);
//		UpdateGeometryColumnInfoRequest req = builder.build();
//		
//		DataSetInfoResponse resp = m_dsBlockingStub.updateGeometryColumnInfo(req);
//		return toDataSet(resp);		
//	}
//	
//	public SpatialIndexInfo getDefaultSpatialIndexInfoOrNull(String dsId) {
//		StringProto req = PBUtils.toStringProto(dsId);
//		return handle(m_dsBlockingStub.getDefaultSpatialIndexInfo(req));
//	}
	
//	public RangeQueryEstimate estimateRangeQuery(String dsId, Envelope range) {
//		EstimateRangeQueryRequest req = EstimateRangeQueryRequest.newBuilder()
//																.setDatasetId(dsId)
//																.setRange(PBUtils.toProto(range))
//																.build();
//		RangeQueryEstimateResponse resp = m_dsBlockingStub.estimateRangeQuery(req);
//		switch ( resp.getEitherCase() ) {
//			case ESTIMATE:
//				return RangeQueryEstimate.fromProto(resp.getEstimate());
//			case ERROR:
//				throw Throwables.toRuntimeException(PBUtils.toException(resp.getError()));
//			default:
//				throw new AssertionError();
//		}
//	}
//	
//	public RecordSet queryRange(String dsId, Envelope range, int nsamples)
//		throws DataSetNotFoundException {
//		StreamDownloadReceiver downloader = new StreamDownloadReceiver();
//		StreamObserver<DownChunkResponse> channel = m_dsStub.queryRange(downloader);
//		
//		QueryRangeRequest req = QueryRangeRequest.newBuilder()
//												.setId(dsId)
//												.setRange(PBUtils.toProto(range))
//												.setSampleCount(nsamples)
//												.setUseCompression(m_marmot.useCompression())
//												.build();
//
//		// start download by sending 'stream-download' request
//		InputStream is = downloader.start(req.toByteString(), channel);
//		if ( m_marmot.useCompression() ) {
//			is = Lz4Compressions.decompress(is);
//		}
//		
//		return PBRecordProtos.readRecordSet(is);
//	}
//	
//	public long appendRecordSet(String dsId, RecordSet rset, FOption<String> partId) {
//		try {
//			InputStream is = PBRecordProtos.toInputStream(rset);
//			if ( m_marmot.useCompression() ) {
//				is = Lz4Compressions.compress(is);
//			}
//			
//			StreamUploadSender uploader = new StreamUploadSender(is) {
//				@Override
//				protected ByteString getHeader() throws Exception {
//					AppendRecordSetRequest.Builder builder
//									= AppendRecordSetRequest.newBuilder()
//															.setId(dsId)
//															.setUseCompression(m_marmot.useCompression());
//					
//					builder = partId.transform(builder, (b,i) -> b.setPartitionId(i));
//					AppendRecordSetRequest req = builder.build();
//					return req.toByteString();
//				}
//			};
//			StreamObserver<UpChunkRequest> channel = m_dsStub.appendRecordSet(uploader);
//			uploader.setChannel(channel);
//			uploader.start();
//			
//			ByteString ret = uploader.get();
//			return LongProto.parseFrom(ret).getValue();
//		}
//		catch ( Throwable e ) {
//			Throwable cause = Throwables.unwrapThrowable(e);
//			throw Throwables.toRuntimeException(cause);
//		}
//	}
//	
////	public PBDataSetProxy appendPlanResult(String dsId, Plan plan, ExecutePlanOptions execOpts)
////		throws DataSetNotFoundException {
////		ExecutePlanRequest execPlan = ExecutePlanRequest.newBuilder()
////														.setPlan(plan.toProto())
////														.setOptions(execOpts.toProto())
////														.build();
////		AppendPlanResultRequest req = AppendPlanResultRequest.newBuilder()
////															.setId(dsId)
////															.setPlanExec(execPlan)
////															.build();
////		DataSetInfoResponse resp = m_dsBlockingStub.appendPlanResult(req);
////		return toDataSet(resp);
////	}
//	
//	public long getDataSetLength(String id) {
//		return PBUtils.handle(m_dsBlockingStub.getDataSetLength(PBUtils.toStringProto(id)));
//	}
//	
//	public void moveDataSet(String srcId, String tarId) {
//		VoidResponse resp = m_dsBlockingStub.moveDataSet(MoveDataSetRequest.newBuilder()
//																	.setSrcId(srcId)
//																	.setDestId(tarId)
//																	.build());
//		PBUtils.handle(resp);
//	}
//
//	public boolean deleteDataSet(String id) {
//		return PBUtils.handle(m_dsBlockingStub.deleteDataSet(PBUtils.toStringProto(id)));
//	}
//	
//	public Set<String> getClusterQuadKeyAll(String id) throws NotSpatiallyClusteredException {
//		return FStream.from(m_dsBlockingStub.getClusterQuadKeyAll(PBUtils.toStringProto(id)))
//						.map(StringResponse::getValue)
//						.toSet();
//	}
//
//	public SpatialIndexInfo createSpatialIndex(String id, CreateSpatialIndexOptions opts) {
//		CreateSpatialIndexRequest req = CreateSpatialIndexRequest.newBuilder()
//														.setId(id)
//														.setOptions(opts.toProto())
//														.build();
//		return handle(m_dsBlockingStub.createSpatialIndex(req));
//	}
//
//	public void deleteSpatialCluster(String id) {
//		PBUtils.handle(m_dsBlockingStub.deleteSpatialIndex(PBUtils.toStringProto(id)));
//	}
//	
//	public RecordSet readSpatialCluster(String dsId, String quadKey) {
//		StreamDownloadReceiver downloader = new StreamDownloadReceiver();
//
//		// start download by sending 'stream-download' request
//		ReadRawSpatialClusterRequest.Builder builder = ReadRawSpatialClusterRequest.newBuilder()
//																.setDatasetId(dsId)
//																.setQuadKey(quadKey)
//																.setUseCompression(m_marmot.useCompression());
//		ReadRawSpatialClusterRequest req = builder.build();
//		StreamObserver<DownChunkResponse> channel = m_dsStub.readRawSpatialCluster(downloader);
//		InputStream is = downloader.start(req.toByteString(), channel);
//		if ( m_marmot.useCompression() ) {
//			is = Lz4Compressions.decompress(is);
//		}
//		return PBRecordProtos.readRecordSet(is);
//	}
//
//	public List<String> getDirAll() {
//		return FStream.from(m_dsBlockingStub.getDirAll(PBUtils.VOID))
//						.map(StringResponse::getValue)
//						.toList();
//	}
//
//	public List<String> getSubDirAll(String folder, boolean recursive) {
//		DirectoryTraverseRequest req = DirectoryTraverseRequest.newBuilder()
//													.setDirectory(folder)
//													.setRecursive(recursive)
//													.build();
//		Iterator<StringResponse> resp = m_dsBlockingStub.getSubDirAll(req);
//		return PBUtils.toFStream(resp)
//						.map(StringResponse::getValue)
//						.toList();
//	}
//
//	public String getParentDir(String folder) {
//		StringProto req = PBUtils.toStringProto(folder);
//		StringResponse resp = m_dsBlockingStub.getParentDir(req);
//		return PBUtils.handle(resp);
//	}
//
//	public void renameDir(String srcPath, String tarPath) {
//		MoveDirRequest req = MoveDirRequest.newBuilder()
//												.setSrcPath(srcPath)
//												.setDestPath(tarPath)
//												.build();
//		VoidResponse resp = m_dsBlockingStub.moveDir(req);
//		PBUtils.handle(resp);
//	}
//
//	public void deleteDir(String folder) {
//		StringProto req = PBUtils.toStringProto(folder);
//		VoidResponse resp = m_dsBlockingStub.deleteDir(req);
//		PBUtils.handle(resp);
//	}
//
//	public long getBlockSize(String id) {
//		LongResponse resp = m_dsBlockingStub.getBlockSize(PBUtils.toStringProto(id));
//		return PBUtils.handle(resp);
//	}
//	
//	public void createKafkaTopic(String topic, boolean force) {
//		CreateKafkaTopicRequest req = CreateKafkaTopicRequest.newBuilder()
//															.setTopic(topic)
//															.setForce(force)
//															.build();
//		PBUtils.handle(m_dsBlockingStub.createKafkaTopic(req));
//	}
//
//	public Set<String> estimateQuadKeys(String dsId, EstimateQuadKeysOptions opts) {
//		EstimateQuadKeysRequest req = EstimateQuadKeysRequest.newBuilder()
//															.setDsId(dsId)
//															.setOptions(opts.toProto())
//															.build();
//		return FStream.from(m_dsBlockingStub.estimateQuadKeys(req))
//						.map(StringResponse::getValue)
//						.toSet();
//	}
//
//	public void cluster(String dsId, String outDsId, ClusterSpatiallyOptions opts) {
//		ClusterSpatiallyRequest.Builder builder = ClusterSpatiallyRequest.newBuilder()
//																		.setDsId(dsId)
//																		.setOptions(opts.toProto());
//		if ( outDsId != null ) {
//			builder = builder.setOutDsId(outDsId);
//		}
//		ClusterSpatiallyRequest req = builder.build();
//		
//		PBUtils.handle(m_dsBlockingStub.clusterSpatially(req));
//	}
//	
//	public boolean hasThumbnail(String dsId) {
//		BoolResponse resp = m_dsBlockingStub.hasThumbnail(PBUtils.toStringProto(dsId));
//		return PBUtils.handle(resp);
//	}
//	
//	public void createThumbnail(String dsId, int sampleCount) {
//		CreateThumbnailRequest req = CreateThumbnailRequest.newBuilder()
//															.setId(dsId)
//															.setCount(sampleCount)
//															.build();
//		PBUtils.handle(m_dsBlockingStub.createThumbnail(req));
//	}
//	
//	public boolean deleteThumbnail(String dsId) {
//		BoolResponse resp = m_dsBlockingStub.deleteThumbnail(PBUtils.toStringProto(dsId));
//		return PBUtils.handle(resp);
//	}
//	
//	public RecordSet readThumbnail(String dsId, Envelope range, int count) {
//		ReadThumbnailRequest req = ReadThumbnailRequest.newBuilder()
//														.setId(dsId)
//														.setRange(PBUtils.toProto(range))
//														.setCount(count)
//														.setUseCompression(m_marmot.useCompression())
//														.build();
//
//		StreamDownloadReceiver downloader = new StreamDownloadReceiver();
//		StreamObserver<DownChunkResponse> channel = m_dsStub.readThumbnail(downloader);
//		InputStream is = downloader.start(req.toByteString(), channel);
//		if ( m_marmot.useCompression() ) {
//			is = Lz4Compressions.decompress(is);
//		}
//
//		return PBRecordProtos.readRecordSet(is);
//	}
//	
//	public float getThumbnailRatio(String dsId) {
//		FloatResponse resp = m_dsBlockingStub.getThumbnailRatio(PBUtils.toStringProto(dsId));
//		return PBUtils.handle(resp);
//	}
	
	GrpcDataSetProxy toDataSet(String id, DataSetInfoResponse resp) {
		switch ( resp.getEitherCase() ) {
			case DATASET_INFO:
				return new GrpcDataSetProxy(this, id, DataSetInfo.fromProto(resp.getDatasetInfo()));
			case ERROR:
				switch ( resp.getError().getCode() ) {
					case NOT_FOUND:
						throw new DataSetNotFoundException(resp.getError().getDetails());
					default:
						throw Throwables.toRuntimeException(PBUtils.toException(resp.getError()));
				}
			default:
				throw new AssertionError();
		}
	}
	
//	private SpatialIndexInfo handle(SpatialIndexInfoResponse resp) {
//		switch ( resp.getEitherCase() ) {
//			case INDEX_INFO:
//				return SpatialIndexInfo.fromProto(resp.getIndexInfo());
//			case NONE:
//				return null;
//			case ERROR:
//				throw Throwables.toRuntimeException(PBUtils.toException(resp.getError()));
//			default:
//				throw new AssertionError();
//		}
//	}
}
