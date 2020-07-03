package marmot.remote.server;

import java.io.File;
import java.io.InputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import io.grpc.stub.StreamObserver;
import marmot.DataSetInfo;
import marmot.RecordSchema;
import marmot.avro.AvroDataSet;
import marmot.avro.AvroUtils;
import proto.StringProto;
import proto.dataset.DataSetInfoResponse;
import proto.dataset.DataSetServiceGrpc.DataSetServiceImplBase;
import proto.stream.DownMessage;
import proto.stream.UpMessage;
import utils.grpc.PBUtils;
import utils.grpc.stream.server.StreamDownloadSender;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GrpcDataSetServiceServant extends DataSetServiceImplBase {
	private static final Logger s_logger = LoggerFactory.getLogger(GrpcDataSetServiceServant.class);
	
	private final ExecutorService m_executor = Executors.newCachedThreadPool();
	
	@Override
    public void getDataSetInfo(StringProto req, StreamObserver<DataSetInfoResponse> channel) {
		try {
			String id = req.getValue();
			RecordSchema schema = AvroDataSet.from(new File(id)).getRecordSchema();
			DataSetInfo info = new DataSetInfo(schema);
			
			channel.onNext(DataSetInfoResponse.newBuilder()
												.setDatasetInfo(info.toProto())
												.build());
		}
		catch ( Exception e ) {
			channel.onNext(DataSetInfoResponse.newBuilder()
												.setError(PBUtils.ERROR(e))
												.build());
		}
		finally {
			channel.onCompleted();
		}
	}

	@Override
    public StreamObserver<UpMessage> readDataSet(StreamObserver<DownMessage> channel) {
		StreamDownloadSender sender = new StreamDownloadSender(channel) {
			@Override
			protected InputStream getStream(ByteString req) throws Exception {
				String path = PBUtils.STRING(req);
				AvroDataSet ds = AvroDataSet.from(new File(path));

				s_logger.trace("download: path={}", path);
				return AvroUtils.readSerializedStream(ds);
			}
		};
		CompletableFuture.runAsync(sender, m_executor);
		
		return sender;
    }
}
