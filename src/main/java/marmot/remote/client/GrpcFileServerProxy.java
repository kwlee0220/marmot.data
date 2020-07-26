package marmot.remote.client;


import static utils.grpc.PBUtils.BYTE_STRING;
import static utils.grpc.PBUtils.STRING;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

import com.google.common.io.ByteStreams;
import com.google.protobuf.ByteString;

import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import marmot.file.FileServer;
import marmot.proto.Int64Response;
import marmot.proto.StringProto;
import marmot.proto.UpMessage;
import proto.file.FileServiceGrpc;
import proto.file.FileServiceGrpc.FileServiceBlockingStub;
import proto.file.FileServiceGrpc.FileServiceStub;
import utils.Throwables;
import utils.grpc.PBUtils;
import utils.grpc.stream.client.StreamDownloadReceiver;
import utils.grpc.stream.client.StreamUploadOutputStream;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GrpcFileServerProxy implements FileServer {
	private final FileServiceBlockingStub m_blockStub;
	private final FileServiceStub m_stub;

	public GrpcFileServerProxy(ManagedChannel channel) {
		m_stub = FileServiceGrpc.newStub(channel);
		m_blockStub = FileServiceGrpc.newBlockingStub(channel);
	}

	@Override
	public InputStream readFile(String path) {
		StreamDownloadReceiver downloader = new StreamDownloadReceiver();

		// start download by sending 'stream-download' request
		StreamObserver<UpMessage> outChannel = m_stub.readHdfsFile(downloader);
		downloader.start(BYTE_STRING(path), outChannel);
		
		return downloader.getDownloadStream();
	}

	@Override
	public long writeFile(String path, InputStream is) throws IOException {
		StreamUploadOutputStream suos = new StreamUploadOutputStream(BYTE_STRING(path));
		try {
			StreamObserver<UpMessage> channel = m_stub.writeHdfsFile(suos);
			suos.setOutgoingChannel(channel);
			
			ByteStreams.copy(is, suos);
			suos.close();
			
			ByteString ret = suos.awaitResult();
			return PBUtils.handle(Int64Response.parseFrom(ret));
		}
		catch ( Throwable e ) {
			Throwable cause = Throwables.unwrapThrowable(e);
			throw Throwables.toRuntimeException(cause);
		}
	}
	
	@Override
	public boolean deleteFile(String path) {
		return PBUtils.handle(m_blockStub.deleteHdfsFile(STRING(path)));
	}

	@Override
	public FStream<String> walkRegularFileTree(String start) {
		Iterator<StringProto> iter = m_blockStub.walkRegularFileTree(STRING(start));
		return FStream.from(iter).map(StringProto::getValue);
	}
}
