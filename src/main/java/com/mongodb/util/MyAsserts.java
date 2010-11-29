package com.mongodb.util;

/**
 * This class is only required to use byte[] as bson value for passing dependency issue.
 * 
 * @author suguru
 */
public class MyAsserts {

	public static final void assertEquals(int v1, int v2) {
		assert v1 == v2;
	}

}
