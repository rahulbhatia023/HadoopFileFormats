package com.rahul.hadoop.fileformats.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.storage.ql.exec.vector.BytesColumnVector;
import org.apache.orc.storage.ql.exec.vector.LongColumnVector;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;

import java.io.IOException;

public class OrcFileReader {
    public static void main(String[] args) throws IOException {
        Reader orcReader = OrcFile.createReader(new Path("/home/rahulbhatia/Rahul_Bhatia/intellij_workspace/HadoopFileFormats/src/main/resources/userdata.orc"),
                OrcFile.readerOptions(new Configuration(true)));

        String compressionCodec = orcReader.getCompressionKind().name();
        int compressionSize = orcReader.getCompressionSize();
        String schema = orcReader.getSchema().toString();
        String fileVersion = orcReader.getFileVersion().getName();
        long numberOfRows = orcReader.getNumberOfRows();

        System.out.println("compressionCodec: " + compressionCodec);
        System.out.println("compressionSize: " + compressionSize);
        System.out.println("schema: " + schema);
        System.out.println("fileVersion: " + fileVersion);
        System.out.println("numberOfRows: " + numberOfRows);

        RecordReader rows = orcReader.rows();
        VectorizedRowBatch batch = orcReader.getSchema().createRowBatch();

        while (rows.nextBatch(batch)) {
            LongColumnVector id = (LongColumnVector) batch.cols[1];
            BytesColumnVector firstName = (BytesColumnVector) batch.cols[2];
            BytesColumnVector lastName = (BytesColumnVector) batch.cols[3];
            BytesColumnVector email = (BytesColumnVector) batch.cols[4];

            for (int r = 0; r < batch.size; r++) {
                System.out.println("id: " + id.vector[r] +
                        ", firstName: " + new String(firstName.vector[r], firstName.start[r], firstName.length[r]) +
                        ", lastName: " + new String(lastName.vector[r], lastName.start[r], lastName.length[r]) +
                        ", email: " + new String(email.vector[r], email.start[r], email.length[r]));
            }
        }
    }
}