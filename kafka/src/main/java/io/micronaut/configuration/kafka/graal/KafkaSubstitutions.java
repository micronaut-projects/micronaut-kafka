package io.micronaut.configuration.kafka.graal;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import com.oracle.svm.core.annotate.Alias;
import com.oracle.svm.core.annotate.Delete;
import com.oracle.svm.core.annotate.RecomputeFieldValue;
import com.oracle.svm.core.annotate.Substitute;
import com.oracle.svm.core.annotate.TargetClass;
import com.oracle.svm.core.annotate.RecomputeFieldValue.Kind;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.record.BufferSupplier;
import org.apache.kafka.common.record.CompressionType;
import static org.apache.kafka.common.record.CompressionType.*;
import org.apache.kafka.common.utils.ByteBufferInputStream;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.GZIPInputStream;
import java.io.BufferedOutputStream;;
import java.io.BufferedInputStream;
import org.apache.kafka.common.record.KafkaLZ4BlockInputStream;
import org.apache.kafka.common.record.KafkaLZ4BlockOutputStream;
import org.apache.kafka.common.record.RecordBatch;

@TargetClass(className = "org.apache.kafka.common.utils.Crc32C$Java9ChecksumFactory")
@Substitute
final class Java9ChecksumFactory {

    @Substitute
    public Checksum create() {
        return new CRC32();
    }

}

@TargetClass(className = "org.apache.kafka.common.record.CompressionType")
final class CompressionTypeSubs {

    // @Alias @RecomputeFieldValue(kind = RecomputeFieldValue.Kind.FromAlias)
    // public static CompressionType LZ4 = CompressionType.GZIP;

    @Alias @RecomputeFieldValue(kind = RecomputeFieldValue.Kind.FromAlias)
    public static CompressionType SNAPPY = CompressionType.GZIP;

    @Alias @RecomputeFieldValue(kind = RecomputeFieldValue.Kind.FromAlias)
    public static CompressionType ZSTD = CompressionType.GZIP;
}
