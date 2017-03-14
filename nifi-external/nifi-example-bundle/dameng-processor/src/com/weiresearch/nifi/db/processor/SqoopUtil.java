package com.weiresearch.nifi.db.processor;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.SqoopOptions.IncrementalMode;
import com.cloudera.sqoop.tool.ImportTool;

public class SqoopUtil {

	private static SqoopOptions SqoopOptions = null;
	
	static{
		Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
		SqoopOptions = new SqoopOptions(conf);
	}


	public static void setUp(String db_driver, String db_url, String db_user,
			String db_password) {


		SqoopOptions.setConnectString(db_url);
		SqoopOptions.setUsername(db_user);
		SqoopOptions.setPassword(db_password);
		SqoopOptions.setDriverClassName(db_driver);
		SqoopOptions.setNumMappers(1);

		SqoopOptions.setFieldsTerminatedBy('\t');
		SqoopOptions.setHadoopMapRedHome("");
		SqoopOptions.setLinesTerminatedBy('\n');
		SqoopOptions.setDeleteMode(true);
		
	}

	@SuppressWarnings("deprecation")
	public static int runIt() {
		int res = 0;
		res = new ImportTool().run(SqoopOptions);
		if (res != 0) {
			throw new RuntimeException("Sqoop API Failed - return code : "
					+ Integer.toString(res));
		}
		return res;
	}

	public static void TransferringEntireTable(String table) {
		SqoopOptions.setTableName(table);
	}

	public static void TransferringEntireTableSpecificDir(String table,
			String directory) {
		TransferringEntireTable(table);
		SqoopOptions.setTargetDir(directory);
	}

	public static void TansferringEntireTableSpecificDirHiveMerge(String table,
			String directory) {
		TransferringEntireTableSpecificDir(table, directory);
		SqoopOptions.setHiveImport(true);
	}

	public static void TansferringEntireTableSpecificDirHivePartitionMerge(
			String table, String directory, String partitionKey,
			String partitionValue) {
		TansferringEntireTableSpecificDirHiveMerge(table, directory);
		SqoopOptions.setHivePartitionKey(partitionKey);
		SqoopOptions.setHivePartitionValue(partitionValue);
	}

	public static void TansferringEntireTableWhereClause(String sqlStatement) {
		SqoopOptions.setSqlQuery(sqlStatement);
		SqoopOptions.setSplitByCol("id");

	}

	public static void CompressingImportedData(String table, String directory,
			String compress) {
		TransferringEntireTableSpecificDir(table, directory);
		SqoopOptions.setCompressionCodec(compress);
	}

	public static void incrementalImport(String table, String directory,
			IncrementalMode mode, String checkColumn, String lastVale) {
		TransferringEntireTableSpecificDir(table, directory);
		SqoopOptions.setIncrementalMode(mode);
		SqoopOptions.setAppendMode(true);
		SqoopOptions.setIncrementalTestColumn(checkColumn);
		SqoopOptions.setIncrementalLastValue(lastVale);
	}

	public static void TransferringEntireTableSpecificDirHive(String table,
			String directory) {

		TransferringEntireTableSpecificDir(table, directory);
		SqoopOptions.setHiveImport(true);
	}

	public static void TransferringEntireTableSpecificDirHivePartition(
			String table, String directory, String partitionKey,
			String partitionValue) {
		TransferringEntireTableSpecificDirHive(table, directory);
		SqoopOptions.setHivePartitionKey(partitionKey);
		SqoopOptions.setHivePartitionValue(partitionValue);
	}


	public static void main(String[] args) throws IOException {

	}

}
