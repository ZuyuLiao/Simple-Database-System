package test;

import static org.junit.Assert.*;

import org.junit.Test;

import hw1.IntField;
import hw1.StringField;
import hw1.Tuple;
import hw1.TupleDesc;
import hw1.Type;

public class TupleTest {

	@Test
	public void testFieldAccess() {
		Type[] t = new Type[] {Type.INT, Type.STRING};
		String[] c = new String[] {"a", "bs"};
		
		TupleDesc td = new TupleDesc(t, c);
		
		Tuple tup = new Tuple(td);
		byte[] f1 = new byte[] {(byte)(Math.random() * 256), (byte)(Math.random() * 256), (byte)(Math.random() * 256), (byte)(Math.random() * 256)};
		byte[] f2 = new byte[129];
		f2[0] = (byte)(Math.random() * 129);
		for(int i = 1; i < f2[0] + 1; i++) {
			f2[i] = (byte)(Math.random() * 256);
		}

		tup.setField(0, new IntField(f1));
		tup.setField(1, new StringField(f2));
		
		assertTrue(tup.getField(0).equals(new IntField(f1)));
		assertTrue(tup.getField(1).equals(new StringField(f2)));

	}


}
