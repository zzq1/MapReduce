package com.neu.hive;

import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

import com.neu.hive.UDF.ToUpperCaseUDF;

public class ToUpperCaseUDFTest {
	/**
	 *  +VE: Test
	 */
	@Test
	public void testToUpperCaseUDF() {
		ToUpperCaseUDF upperCase = new ToUpperCaseUDF();
		Assert.assertEquals("HELLO WORLD", upperCase.evaluate(new Text("Hello world")).toString());
	}

	/**
	 *  -VE: Test
	 */
	@Test
	public void testToUpperCaseUDFNullCheck() {
		ToUpperCaseUDF upperCase = new ToUpperCaseUDF();
		Assert.assertNull(upperCase.evaluate(null));
	}
}
