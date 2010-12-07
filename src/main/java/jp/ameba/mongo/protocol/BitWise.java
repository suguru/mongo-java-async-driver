package jp.ameba.mongo.protocol;

public class BitWise {
	
	/*
	private static final int[] BIT_ARRAY = new int[] {
		0x00000001, 0x00000002, 0x00000004, 0x00000008,
		0x00000010, 0x00000020, 0x00000040, 0x00000080,
		0x00000100, 0x00000200, 0x00000400, 0x00000800,
		0x00001000, 0x00002000, 0x00004000, 0x00008000,
		0x00010000, 0x00020000, 0x00040000, 0x00080000,
		0x00100000, 0x00200000, 0x00400000, 0x00800000,
		0x01000000, 0x02000000, 0x04000000, 0x08000000,
		0x10000000, 0x20000000, 0x40000000, 0x80000000
	};
	*/

	/**
	 * 指定の箇所にビットをもつ int を返却します。
	 * @param indexes
	 * @return
	 */
	public static final int bits(int ... indexes) {
		
		int bits = 0;
		for (int i = 0; i < indexes.length; i++) {
			bits = bits | (1 << indexes[i]);
		}
		return bits;
		
	}
	
	/**
	 * 指定の int に特定の番号のビットを付け足します。
	 * @param bits
	 * @param index
	 * @return
	 */
	public static final int addBit(int bits, int index) {
		bits = bits | (1 << index);	
		return bits;
	}
	
	/**
	 * 指定の int の特定の番号ビットが 1 であるかチェックします。
	 * @param bits
	 * @param index
	 * @return
	 */
	public static final boolean hasBit(int bits, int index) {
		return (bits & (1 << index)) != 0;
	}

}
