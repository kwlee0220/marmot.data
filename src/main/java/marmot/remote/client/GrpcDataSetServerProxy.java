package marmot.remote.client;

import static utils.grpc.PBUtils.BYTE_STRING;
import static utils.grpc.PBUtils.STRING;
import static utils.grpc.PBUtils.VOID;

import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.UnaryOperator;

import org.apache.avro.Schema;

import com.google.protobuf.ByteString;

import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import marmot.RecordStream;
import marmot.avro.AvroBinaryRecordWriter;
import marmot.avro.AvroUtils;
import marmot.dataset.DataSet;
import marmot.dataset.DataSetExistsException;
import marmot.dataset.DataSetInfo;
import marmot.dataset.DataSetNotFoundException;
import marmot.dataset.DataSetServer;
import marmot.dataset.proto.CreateDataSetRequest;
import marmot.dataset.proto.DataSetInfoProto;
import marmot.dataset.proto.DataSetInfoResponse;
import marmot.dataset.proto.DataSetServiceGrpc;
import marmot.dataset.proto.DataSetServiceGrpc.DataSetServiceBlockingStub;
import marmot.dataset.proto.DataSetServiceGrpc.DataSetServiceStub;
import marmot.dataset.proto.DirectoryTraverseRequest;
import marmot.dataset.proto.MoveDataSetRequest;
import marmot.dataset.proto.MoveDirRequest;
import marmot.proto.StringProto;
import marmot.proto.StringResponse;
import marmot.proto.UpMessage;
import marmot.proto.VoidResponse;
import utils.Throwables;
import utils.grpc.AlreadyExistsException;
import utils.grpc.PBUtils;
import utils.grpc.stream.client.StreamDownloadReceiver;
import utils.grpc.stream.client.StreamUploadOutputStream;
import utils.grpc.stream.client.StreamUploadSender;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GrpcDataSetServerProxy implements DataSetServer {
	private final DataSetServiceBlockingStub m_blockStub;
	private final DataSetServiceStub m_stub;
	private UnaryOperator<DataSet> m_interceptor;

	public GrpcDataSetServerProxy(ManagedChannel channel) {
		m_stub = DataSetServiceGrpc.newStub(channel);
		m_blockStub = DataSetServiceGrpc.newBlockingStub(channel);
	}
	
	public void setDataSetInterceptor(UnaryOperator<DataSet> interceptor) {
		m_interceptor = interceptor;
	}
	
	@Override
	public DataSet createDataSet(DataSetInfo dsInfo, boolean force) throws DataSetExistsException {
		CreateDataSetRequest req  = CreateDataSetRequest.newBuilder()
														.setId(dsInfo.getId())
														.setType(dsInfo.getType().name())
														.setRecordSchema(dsInfo.getRecordSchema().toTypeId())
														.setForce(force)
														.build();
		try {
			return toDataSet(m_blockStub.createDataSet(req));
		}
		catch ( AlreadyExistsException e ) {
			throw new DataSetExistsException(e.getLocalizedMessage());
		}
	}
	
	@Override
	public boolean deleteDataSet(String id) {
		return PBUtils.handle(m_blockStub.deleteDataSet(STRING(id)));
	}
	
	@Override
	public DataSet getDataSet(String dsId) {
		DataSetInfoResponse resp = m_blockStub.getDataSetInfo(STRING(dsId));
		return toDataSet(resp);
	}
	
	@Override
	public DataSet getDataSetOrNull(String dsId) {
		DataSetInfoResponse resp = m_blockStub.getDataSetInfo(STRING(dsId));
		try {
			return toDataSet(resp);
		}
		catch ( DataSetNotFoundException e ) {
			return null;
		}
	}

	@Override
	public DataSet updateDataSet(DataSetInfo info) {
		DataSetInfoResponse resp = m_blockStub.updateDataSetInfo(info.toProto());
		return toDataSet(resp);
	}
	
	@Override
	public List<DataSet> getDataSetAll() {
		return FStream.from(m_blockStub.getDataSetInfoAll(VOID()))
						.map(this::toDataSet)
						.cast(DataSet.class)
						.toList();
	}
	
	@Override
	public List<DataSet> getDataSetAllInDir(String folder, boolean recursive) {
		DirectoryTraverseRequest req = DirectoryTraverseRequest.newBuilder()
													.setDirectory(folder)
													.setRecursive(recursive)
													.build();
		return FStream.from(m_blockStub.getDataSetInfoAllInDir(req))
						.map(this::toDataSet)
						.cast(DataSet.class)
						.toList();
	}
	
	@Override
	public DataSet moveDataSet(String id, String newId) {
		DataSetInfoResponse resp = m_blockStub.moveDataSet(MoveDataSetRequest.newBuilder()
																			.setSrcId(id)
																			.setDestId(newId)
																			.build());
		return toDataSet(resp);
	}
	
	@Override
	public Set<String> getDirAll() {
		return FStream.from(m_blockStub.getDirAll(VOID()))
						.map(PBUtils::handle)
						.toSet();
	}
	
	@Override
	public Set<String> getSubDirAll(String folder, boolean recursive) {
		DirectoryTraverseRequest req = DirectoryTraverseRequest.newBuilder()
																.setDirectory(folder)
																.setRecursive(recursive)
																.build();
		Iterator<StringResponse> resp = m_blockStub.getSubDirAll(req);
		return FStream.from(resp)
						.map(PBUtils::handle)
						.toSet();
	}
	
	@Override
	public void deleteDir(String folder) {
		StringProto req = STRING(folder);
		VoidResponse resp = m_blockStub.deleteDir(req);
		PBUtils.handle(resp);
	}
	
	@Override
	public String getParentDir(String folder) {
		StringProto req = STRING(folder);
		StringResponse resp = m_blockStub.getParentDir(req);
		return PBUtils.handle(resp);
	}
	
	@Override
	public void moveDir(String path, String newPath) {
		MoveDirRequest req = MoveDirRequest.newBuilder()
											.setSrcPath(path)
											.setDestPath(newPath)
											.build();
		VoidResponse resp = m_blockStub.moveDir(req);
		PBUtils.handle(resp);
	}

	public InputStream readDataSet(String id) throws DataSetNotFoundException {
		StreamDownloadReceiver downloader = new StreamDownloadReceiver();

		// start download by sending 'stream-download' request
		StreamObserver<UpMessage> outChannel = m_stub.readDataSet(downloader);
		downloader.start(BYTE_STRING(id), outChannel);
		
		return downloader.getDownloadStream();
	}
	
	public DataSetInfo writeDataSet(String dsId, RecordStream stream) {
		try {
			InputStream is = AvroUtils.toSerializedInputStream(stream);

			StreamUploadSender uploader = new StreamUploadSender(BYTE_STRING(dsId), is);
			uploader.setChannel(m_stub.writeDataSet(uploader));
			
			ByteString ret = uploader.run();
			return DataSetInfo.fromProto(DataSetInfoProto.parseFrom(ret));
		}
		catch ( Throwable e ) {
			Throwable cause = Throwables.unwrapThrowable(e);
			throw Throwables.toRuntimeException(cause);
		}
	}
	
	public DataSetInfo writeDataSet2(String dsId, RecordStream stream) {
		StreamUploadOutputStream suos = new StreamUploadOutputStream(BYTE_STRING(dsId));
		try {
			StreamObserver<UpMessage> channel = m_stub.writeDataSet(suos);
			suos.setOutgoingChannel(channel);

			Schema avroSchema = AvroUtils.toSchema(stream.getRecordSchema());
			AvroBinaryRecordWriter ser = new AvroBinaryRecordWriter(avroSchema, suos);
			ser.write(stream);
			suos.close();
			
			ByteString ret = suos.awaitResult();
			return DataSetInfo.fromProto(DataSetInfoProto.parseFrom(ret));
		}
		catch ( Throwable e ) {
			Throwable cause = Throwables.unwrapThrowable(e);
			throw Throwables.toRuntimeException(cause);
		}
	}
	
	DataSet toDataSet(DataSetInfoResponse resp) {
		switch ( resp.getEitherCase() ) {
			case DATASET_INFO:
				DataSetInfo dsInfo = DataSetInfo.fromProto(resp.getDatasetInfo());
				DataSet ds = new GrpcDataSetProxy(this, dsInfo);
				if ( m_interceptor != null ) {
					ds = m_interceptor.apply(ds);
				}
				return ds;
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
}
