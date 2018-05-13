package com.neu.hive.UDF;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

@Description(name = "find_in_string", value = "_FUNC_(needle, haystack) - Return the index at which "
		+ "needle appears in haystack.")
public class UDFFindInString  extends UDF{
	public Integer evaluate(String needle, String haystack) {
		if (haystack == null || needle == null) {
			return null;
		}
		return haystack.indexOf(needle);
	}
}
