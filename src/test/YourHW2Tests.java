package test;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;

import org.junit.Before;
import org.junit.Test;

import hw1.Catalog;
import hw1.Database;
import hw1.HeapFile;
import hw1.IntField;
import hw1.StringField;
import hw1.Tuple;
import hw1.TupleDesc;
import hw2.AggregateOperator;
import hw2.Query;
import hw2.Relation;

public class YourHW2Tests {

	private HeapFile testhf;
	private TupleDesc testtd;
	private HeapFile ahf;
	private TupleDesc atd;
	private Catalog c;

	@Before
	public void setup() {
		
		try {
			Files.copy(new File("testfiles/test.dat.bak").toPath(), new File("testfiles/test.dat").toPath(), StandardCopyOption.REPLACE_EXISTING);
			Files.copy(new File("testfiles/A.dat.bak").toPath(), new File("testfiles/A.dat").toPath(), StandardCopyOption.REPLACE_EXISTING);
		} catch (IOException e) {
			System.out.println("unable to copy files");
			e.printStackTrace();
		}
		
		c = Database.getCatalog();
		c.loadSchema("testfiles/test.txt");
		
		int tableId = c.getTableId("test");
		testtd = c.getTupleDesc(tableId);
		testhf = c.getDbFile(tableId);
		
		c = Database.getCatalog();
		c.loadSchema("testfiles/A.txt");
		
		tableId = c.getTableId("A");
		atd = c.getTupleDesc(tableId);
		ahf = c.getDbFile(tableId);
	}
	
	@Test
	public void testAggregate() throws Exception {
		Tuple t = new Tuple(atd);
		t.setField(0, new IntField(new byte[] {0, 0, 0, (byte)1}));
		t.setField(1, new IntField(new byte[] {0, 0, 0, (byte)1}));
		ahf.addTuple(t);
		Relation ar = new Relation(ahf.getAllTuples(), atd);
		ArrayList<Integer> c = new ArrayList<Integer>();
		c.add(1);
		ar = ar.project(c);
		ar = ar.aggregate(AggregateOperator.SUM, false);
		
		assertTrue(ar.getTuples().size() == 1);
		IntField agg = (IntField) ar.getTuples().get(0).getField(0);
		assertTrue(agg.getValue() == 37);
	}
	
	@Test
	public void testSelect() throws Exception {
		Tuple t = new Tuple(atd);
		t.setField(0, new IntField(new byte[] {0, 0, 0, (byte)22}));
		t.setField(1, new IntField(new byte[] {0, 0, 0, (byte)22}));
		ahf.addTuple(t);
		Query q = new Query("SELECT a1, a2 FROM A WHERE a1 = 22");
		Relation r = q.execute();
		
		assertTrue(r.getTuples().size() == 1);
		assertTrue(r.getDesc().getSize() == 8);
	}
	
	

}
