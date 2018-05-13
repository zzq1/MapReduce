package com.topK;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSUtils {
	
	private FileSystem fs = null;
	
	public HDFSUtils(Configuration conf){
		try {
			fs = FileSystem.newInstance(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void deleteDir(String path) throws IOException {
		Path p = new Path(path);
		if(fs.exists(p)) {
			fs.delete(p, true);
		}
	}

	public void rename(String src, String dest) throws IOException {
		Path p = new Path(src);
		Path destPath = new Path(dest);
		if(fs.exists(p)) {
			this.deleteDir(dest);
			fs.rename(p, destPath);
		}
	}
/*	public static void main(String[] args) {
		HDFSUtils hdfsUtils = new HDFSUtils(conf)
		
	}*/
}
