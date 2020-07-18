package marmot.remote.server;

import static utils.grpc.PBUtils.BOOL_RESPONSE;
import static utils.grpc.PBUtils.ERROR;

import java.io.InputStream;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import io.grpc.stub.StreamObserver;
import marmot.dataset.DataSetExistsException;
import marmot.dataset.DataSetInfo;
import marmot.dataset.DataSetNotFoundException;
import marmot.file.FileServer;
import proto.BoolResponse;
import proto.ErrorProto;
import proto.ErrorProto.Code;
import proto.StringProto;
import proto.dataset.DataSetInfoResponse;
import proto.file.FileServiceGrpc.FileServiceImplBase;
import proto.stream.DownMessage;
import proto.stream.UpMessage;
import utils.grpc.PBUtils;
import utils.grpc.stream.server.StreamDownloadSender;
import utils.grpc.stream.server.StreamUploadReceiver;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GrpcFileServiceServant extends FileServiceImplBase {
	private static final Logger s_logger = LoggerFactory.getLogger(GrpcFileServiceServant.class);
	
	private final FileServer m_server;
	
	public GrpcFileServiceServant(FileServer server) {
		m_server = server;
	}

	@Override
    public StreamObserver<UpMessage> readHdfsFile(StreamObserver<DownMessage> channel) {
		StreamDownloadSender sender = new StreamDownloadSender(channel) {
			@Override
			protected InputStream getStream(ByteString req) throws Exception {
				String path = PBUtils.STRING(req);
				
				s_logger.trace("read: file={}", path);
				return m_server.readFile(path);
			}
		};
		CompletableFuture.runAsync(sender);
		
		return sender;
    }
	
	@Override
    public StreamObserver<UpMessage> writeHdfsFile(StreamObserver<DownMessage> out) {
		return new StreamUploadReceiver(out) {
			@Override
			protected ByteString consumeStream(ByteString header, InputStream is) throws Exception {
				String path = StringProto.parseFrom(header).getValue();

				s_logger.debug("write file={}...", path);
				long size = m_server.writeFile(path, is);
				return PBUtils.INT64(size).toByteString();
			}
		};
	}
	
	@Override
    public void deleteHdfsFile(StringProto req, StreamObserver<BoolResponse> out) {
		try {
			String path = req.getValue();
			boolean done = m_server.deleteFile(path);
			
			out.onNext(BOOL_RESPONSE(done));
		}
		catch ( Exception e ) {
			out.onNext(BOOL_RESPONSE(e));
		}
		finally {
			out.onCompleted();
		}
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
}
