package marmot.remote.server;

import static utils.grpc.PBUtils.BOOL_RESPONSE;
import static utils.grpc.PBUtils.ERROR;
import static utils.grpc.PBUtils.STRING_RESPONSE;
import static utils.grpc.PBUtils.VOID_RESPONSE;

import java.io.InputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import io.grpc.stub.StreamObserver;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordStream;
import marmot.avro.AvroDeserializer;
import marmot.avro.AvroUtils;
import marmot.dataset.DataSet;
import marmot.dataset.DataSetExistsException;
import marmot.dataset.DataSetInfo;
import marmot.dataset.DataSetNotFoundException;
import marmot.dataset.DataSetServer;
import marmot.dataset.proto.CreateDataSetRequest;
import marmot.dataset.proto.DataSetInfoProto;
import marmot.dataset.proto.DataSetInfoResponse;
import marmot.dataset.proto.DataSetServiceGrpc.DataSetServiceImplBase;
import marmot.dataset.proto.DirectoryTraverseRequest;
import marmot.dataset.proto.MoveDataSetRequest;
import marmot.dataset.proto.MoveDirRequest;
import marmot.pb.PBValueProtos;
import marmot.proto.BoolResponse;
import marmot.proto.DownMessage;
import marmot.proto.ErrorProto;
import marmot.proto.ErrorProto.Code;
import marmot.proto.RecordProto;
import marmot.proto.RecordResponse;
import marmot.proto.StringProto;
import marmot.proto.StringResponse;
import marmot.proto.UpMessage;
import marmot.proto.VoidProto;
import marmot.proto.VoidResponse;
import utils.grpc.PBUtils;
import utils.grpc.stream.server.StreamDownloadSender;
import utils.grpc.stream.server.StreamUploadReceiver;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GrpcDataSetServiceServant extends DataSetServiceImplBase {
	private static final Logger s_logger = LoggerFactory.getLogger(GrpcDataSetServiceServant.class);
	
	private final DataSetServer m_server;
	private final ExecutorService m_executor = Executors.newCachedThreadPool();
	
	public GrpcDataSetServiceServant(DataSetServer server) {
		m_server = server;
	}

	@Override
    public void createDataSet(CreateDataSetRequest req, StreamObserver<DataSetInfoResponse> out) {
		String id = req.getId();
		try {
			RecordSchema schema = RecordSchema.fromTypeId(req.getRecordSchema());
			boolean force = req.getForce();
			
			DataSetInfo dsInfo = new DataSetInfo(id, schema);
			DataSetInfo created = m_server.createDataSet(dsInfo, force).getDataSetInfo();
			
			out.onNext(toDataSetInfoResponse(created));
		}
		catch ( Exception e ) {
			out.onNext(toDataSetInfoResponse(e));
		}
		finally {
			out.onCompleted();
		}
	}
	
	@Override
    public void deleteDataSet(StringProto req, StreamObserver<BoolResponse> out) {
		try {
			String id = req.getValue();
			boolean done = m_server.deleteDataSet(id);
			
			out.onNext(BOOL_RESPONSE(done));
		}
		catch ( Exception e ) {
			out.onNext(BOOL_RESPONSE(e));
		}
		finally {
			out.onCompleted();
		}
	}
	
	@Override
    public void getDataSetInfo(StringProto req, StreamObserver<DataSetInfoResponse> out) {
		String id = req.getValue();
		try {
			DataSet ds = m_server.getDataSet(id);
			out.onNext(toDataSetInfoResponse(ds.getDataSetInfo()));
		}
		catch ( Exception e ) {
			out.onNext(toDataSetInfoResponse(e));
		}
		finally {
			out.onCompleted();
		}
	}
	
	@Override
    public void getDataSetInfoAll(VoidProto req, StreamObserver<DataSetInfoResponse> out) {
		try {
			FStream.from(m_server.getDataSetAll())
					.map(DataSet::getDataSetInfo)
					.map(this::toDataSetInfoResponse)
					.forEach(out::onNext);
		}
		catch ( Exception e ) {
			out.onNext(toDataSetInfoResponse(e));
		}
		finally {
			out.onCompleted();
		}
	}
	
	@Override
    public void getDataSetInfoAllInDir(DirectoryTraverseRequest req, StreamObserver<DataSetInfoResponse> out) {
		try {
			FStream.from(m_server.getDataSetAllInDir(req.getDirectory(), req.getRecursive()))
					.map(DataSet::getDataSetInfo)
					.map(this::toDataSetInfoResponse)
					.forEach(out::onNext);
		}
		catch ( Exception e ) {
			out.onNext(toDataSetInfoResponse(e));
		}
		finally {
			out.onCompleted();
		}
	}
	
	@Override
    public void updateDataSetInfo(DataSetInfoProto req, StreamObserver<DataSetInfoResponse> out) {
		try {
			DataSetInfo info = DataSetInfo.fromProto(req);
			DataSet updated = m_server.updateDataSet(info);
			out.onNext(toDataSetInfoResponse(updated.getDataSetInfo()));
		}
		catch ( Exception e ) {
			out.onNext(toDataSetInfoResponse(e));
		}
		finally {
			out.onCompleted();
		}
	}

	@Override
    public void moveDataSet(MoveDataSetRequest req, StreamObserver<DataSetInfoResponse> out) {
		try {
			s_logger.debug("moveDataSet: from={}, to={}", req.getSrcId(), req.getDestId());
			DataSet renamed = m_server.moveDataSet(req.getSrcId(), req.getDestId());
			out.onNext(toDataSetInfoResponse(renamed.getDataSetInfo()));
		}
		catch ( Exception e ) {
			out.onNext(toDataSetInfoResponse(e));
		}
		finally {
			out.onCompleted();
		}
    }
	
	@Override
    public void getDirAll(VoidProto req, StreamObserver<StringResponse> out) {
		try {
			FStream.from(m_server.getDirAll())
					.map(PBUtils::STRING_RESPONSE)
					.forEach(out::onNext);
		}
		catch ( Exception e ) {
			out.onNext(STRING_RESPONSE(e));
		}
		finally {
			out.onCompleted();
		}
	}
	
	@Override
    public void getSubDirAll(DirectoryTraverseRequest req, StreamObserver<StringResponse> out) {
		try {
			FStream.from(m_server.getSubDirAll(req.getDirectory(), req.getRecursive()))
					.map(PBUtils::STRING_RESPONSE)
					.forEach(out::onNext);
		}
		catch ( Exception e ) {
			out.onNext(STRING_RESPONSE(e));
		}
		finally {
			out.onCompleted();
		}
	}
	
	@Override
    public void getParentDir(StringProto req, StreamObserver<StringResponse> out) {
		try {
			String dir = m_server.getParentDir(req.getValue());
			out.onNext(STRING_RESPONSE(dir));
		}
		catch ( Exception e ) {
			out.onNext(STRING_RESPONSE(e));
		}
		finally {
			out.onCompleted();
		}
	}
	
	@Override
    public void deleteDir(StringProto req, StreamObserver<VoidResponse> out) {
		try {
			s_logger.debug("deleteDir: dir={}", req.getValue());
			
			m_server.deleteDir(req.getValue());
			out.onNext(VOID_RESPONSE());
		}
		catch ( Exception e ) {
			out.onNext(VOID_RESPONSE(e));
		}
		finally {
			out.onCompleted();
		}
	}
	
	@Override
    public void moveDir(MoveDirRequest req, StreamObserver<VoidResponse> out) {
		try {
			String srcPath = req.getSrcPath();
			String tarPath = req.getDestPath();

			s_logger.debug("moveDir: from={} to={}", srcPath,  tarPath);
			m_server.moveDir(srcPath, tarPath);
			out.onNext(VOID_RESPONSE());
		}
		catch ( Exception e ) {
			out.onNext(VOID_RESPONSE(e));
		}
		finally {
			out.onCompleted();
		}
	}

	@Override
    public StreamObserver<UpMessage> readDataSet(StreamObserver<DownMessage> channel) {
		StreamDownloadSender sender = new StreamDownloadSender(channel) {
			@Override
			protected InputStream getStream(ByteString req) throws Exception {
				String dsId = PBUtils.STRING(req);
				DataSet ds = m_server.getDataSet(dsId);

				s_logger.trace("download: dataset={}", dsId);
				return AvroUtils.toSerializedInputStream(ds.read());
			}
		};
		CompletableFuture.runAsync(sender, m_executor);
		
		return sender;
    }

	@Override
    public void readDataSet2(StringProto req, StreamObserver<RecordResponse> out) {
		try {
			String dsId = req.getValue();
			DataSet ds = m_server.getDataSet(dsId);
			try ( RecordStream strm = ds.read() ) {
				for ( Record rec = strm.next(); rec != null; rec = strm.next() ) {
					RecordProto recProto = PBValueProtos.toRecordProto(rec);
					RecordResponse resp = RecordResponse.newBuilder().setRecord(recProto).build();
					out.onNext(resp);
				}
			}
		}
		catch ( Exception e ) {
			out.onNext(toRecordResponse(e));
		}
		finally {
			out.onCompleted();
		}
    }
	
	@Override
    public StreamObserver<UpMessage> writeDataSet(StreamObserver<DownMessage> out) {
		return new StreamUploadReceiver(out) {
			@Override
			protected ByteString consumeStream(ByteString header, InputStream is) throws Exception {
				String dsId = StringProto.parseFrom(header).getValue();
				DataSet ds = m_server.getDataSet(dsId);
				RecordSchema schema = ds.getRecordSchema();

				s_logger.debug("writing dataset: id={}...", dsId);
				RecordStream input = AvroDeserializer.deserialize(schema, is);
				ds.write(input);
				
				return ds.getDataSetInfo().toProto().toByteString();
			}
		};
	}
	
	private DataSetInfoResponse toDataSetInfoResponse(DataSetInfo info) {
		return DataSetInfoResponse.newBuilder()
									.setDatasetInfo(info.toProto())
									.build();
	}
	
	private DataSetInfoResponse toDataSetInfoResponse(Exception error) {
		ErrorProto proto;
		if ( error instanceof DataSetNotFoundException )  {
			proto = ERROR(Code.NOT_FOUND, error.getMessage());
		}
		else if ( error instanceof DataSetExistsException )  {
			proto = ERROR(Code.ALREADY_EXISTS, error.getMessage());
		}
		else {
			proto = ERROR(error);
		}
		
		return DataSetInfoResponse.newBuilder().setError(proto).build();
	}
	
	private RecordResponse toRecordResponse(Exception error) {
		ErrorProto proto;
		if ( error instanceof DataSetNotFoundException )  {
			proto = ERROR(Code.NOT_FOUND, error.getMessage());
		}
		else if ( error instanceof DataSetExistsException )  {
			proto = ERROR(Code.ALREADY_EXISTS, error.getMessage());
		}
		else {
			proto = ERROR(error);
		}
		
		return RecordResponse.newBuilder().setError(proto).build();
	}
}
