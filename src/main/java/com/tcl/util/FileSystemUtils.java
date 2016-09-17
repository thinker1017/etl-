package com.tcl.util;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FileSystemUtils {
	@SuppressWarnings("deprecation")
	public static Float getFolderLen(FileSystem fs, Path p) {
		Float result = 0f;
		try {
			if (fs.getFileStatus(p).isDir()) {
				for (FileStatus file : fs.listStatus(p)) {
					result += file.getLen();
				}
				System.out.println(result);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return result;
	}
}
