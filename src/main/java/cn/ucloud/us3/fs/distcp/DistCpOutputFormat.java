package cn.ucloud.us3.fs.distcp;

import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

/**
 * @Name: cn.ucloud.us3.fs.distcp
 * @Description: TODO
 * @Author: rick.wu
 * @E-mail: rick.wu@ucloud.cn
 * @Date: 17:37
 */
public class DistCpOutputFormat<K,V> extends SequenceFileOutputFormat<K,V> {
    @Override
    public RecordWriter getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        final SequenceFile.Writer out = getSequenceWriter(context, context.getOutputKeyClass(), context.getOutputValueClass());
        return new RecordWriter<K, V>() {
            public void write(K key, V value) throws IOException {
                out.append(key, value);
                // 如果没有写入该同步点，会导致拆分任务不均匀，甚至有同一个文件被多个任务操作
                out.sync();
            }

            public void close(TaskAttemptContext context) throws IOException {
                out.close();
            }
        };
    }
}
