package hw1;

import java.io.File;
import java.io.*;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.nio.ByteBuffer;;

/**
 * A heap file stores a collection of tuples. It is also responsible for managing pages.
 * It needs to be able to manage page creation as well as correctly manipulating pages
 * when tuples are added or deleted.
 * @author Sam Madden modified by Doug Shook
 *
 */
public class HeapFile {
	
	public static final int PAGE_SIZE = 4096;
	public File file; //private
	private TupleDesc tupledesc;
	/**
	 * Creates a new heap file in the given location that can accept tuples of the given type
	 * @param f location of the heap file
	 * @param types type of tuples contained in the file
	 */
	public HeapFile(File f, TupleDesc type) {
		//your code here
		file = f;
		tupledesc = type;
		
		
	}
	
	public File getFile() {
		//your code here
		return this.file;
	}
	
	public TupleDesc getTupleDesc() {
		//your code here
		return this.tupledesc;
	}
	
	/**
	 * Creates a HeapPage object representing the page at the given page number.
	 * Because it will be necessary to arbitrarily move around the file, a RandomAccessFile object
	 * should be used here.
	 * @param id the page number to be retrieved
	 * @return a HeapPage at the given page number
	 */
	public HeapPage readPage(int id) {
		//your code here
		
		try {
            RandomAccessFile f = new RandomAccessFile(this.file,"r");
            int offset = HeapFile.PAGE_SIZE * id;
            byte[] data = new byte[HeapFile.PAGE_SIZE];
            if (offset + HeapFile.PAGE_SIZE > f.length()) {
                System.err.println("Page offset exceeds max size");
                f.close();
                return null;
                //System.exit(1);
                
            }
            f.seek(offset);
            f.readFully(data);
            f.close();
            return new HeapPage(id, data, getId());
        } catch (FileNotFoundException e) {
            System.err.println("FileNotFoundException: " + e.getMessage());
            throw new IllegalArgumentException();
        } catch (IOException e) {
            System.err.println("Caught IOException: " + e.getMessage());
            throw new IllegalArgumentException();
        }
	}
	
	/**
	 * Returns a unique id number for this heap file. Consider using
	 * the hash of the File itself.
	 * @return
	 */
	public int getId() {
		//your code here
		return file.getAbsoluteFile().hashCode();
	}
	
	/**
	 * Writes the given HeapPage to disk. Because of the need to seek through the file,
	 * a RandomAccessFile object should be used in this method.
	 * @param p the page to write to disk
	 * @throws IOException 
	 */
	public void writePage(HeapPage p) throws IOException {
		//your code here
		RandomAccessFile raf = new RandomAccessFile(this.file, "rw");
    	int offset = HeapFile.PAGE_SIZE * p.getId();
    	raf.seek(offset);
    	raf.write(p.getPageData(), 0, HeapFile.PAGE_SIZE);
    	raf.close();
	}
	
	/**
	 * Adds a tuple. This method must first find a page with an open slot, creating a new page
	 * if all others are full. It then passes the tuple to this page to be stored. It then writes
	 * the page to disk (see writePage)
	 * @param t The tuple to be stored
	 * @return The HeapPage that contains the tuple
	 * @throws Exception 
	 */
	public HeapPage addTuple(Tuple t) throws Exception {
		//your code here
		if(t == null)
			throw new Exception("tuple to add is null");
		HeapPage page = getEmptyPage();
		if(page != null)
		{
			page.addTuple(t);
			writePage(page);
			return page;
		}
		HeapPage newPage = new HeapPage(getNumPages(),HeapPage.createEmptyPageData() ,this.getId());
		newPage.addTuple(t);
		RandomAccessFile raf = new RandomAccessFile(this.file, "rw");
        int offset = HeapFile.PAGE_SIZE * getNumPages();
        //numPages++;
        raf.seek(offset);
        byte[] newHeapPageData = newPage.getPageData();
        raf.write(newHeapPageData, 0, HeapFile.PAGE_SIZE);
        raf.close();
        return newPage;
	}
	
	private HeapPage getEmptyPage()
	{
		for(int i =0; i< getNumPages(); i++)
		{
			HeapPage hpage = readPage(i);
			if(hpage.getNumEmptySlots()>0)
				return hpage;
		}
		return null;
	}
	/**
	 * This method will examine the tuple to find out where it is stored, then delete it
	 * from the proper HeapPage. It then writes the modified page to disk.
	 * @param t the Tuple to be deleted
	 * @throws Exception 
	 */
	public void deleteTuple(Tuple t) throws Exception{
		//your code here
		HeapPage page = readPage(t.getPid());
		page.deleteTuple(t);
		writePage(page);
	}
	
	/**
	 * Returns an ArrayList containing all of the tuples in this HeapFile. It must
	 * access each HeapPage to do this (see iterator() in HeapPage)
	 * @return
	 */
	public ArrayList<Tuple> getAllTuples() {
		//your code here
		ArrayList<Tuple> sum = new ArrayList<Tuple>(); 
		for(int i =0; i < getNumPages(); i++)
		{
			Iterator<Tuple> j = readPage(i).iterator();
			while(j.hasNext())
			{
			sum.add((Tuple) (j.next()));
			}
		}
		return sum;
	}
	
	/**
	 * Computes and returns the total number of pages contained in this HeapFile
	 * @return the number of pages
	 */
	public int getNumPages() {
		//your code here
		return (int) file.length()/HeapFile.PAGE_SIZE;
	}
}
