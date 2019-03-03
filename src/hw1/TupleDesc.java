package hw1;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc {

	private Type[] types;
	private String[] fields;
	
    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr array specifying the number of and types of fields in
     *        this TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
    	//your code here
    	this.types=typeAr;
    	this.fields = fieldAr;
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        //your code here
    	return fields.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        //your code here
    	int size = this.fields.length;
    	if(i >= size || i < 0)
    	{
    		throw new NoSuchElementException("index out of bound");
    	}
    	if(fields[i] != null)
    	{
    		return fields[i];
    	}
    	throw new NoSuchElementException("no such file");
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int nameToId(String name) throws NoSuchElementException {
        //your code here
    	int size = this.fields.length;
    	if(name == null)
    	{
    		throw new NoSuchElementException();
    	}
    	for(int i =0; i < size; i++)
    	{
    		if(fields[i].equals(name))
    		{
    			return i;
    		}
    	}
    	throw new NoSuchElementException("No such element exist");
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getType(int i) throws NoSuchElementException {
        //your code here
    	int size = this.types.length;
    	if( i < 0 || i>= size)
    	{
    		throw new NoSuchElementException();
    	}
    	return types[i];
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
    	//your code here
    	int size = types.length;
    	int total = 0;
    	for(int i = 0; i <size; i++)
    	{
    		if(types[i] == Type.INT)
    		{
    			total = total + 4;
    		}
    		else if(types[i] == Type.STRING)
    		{
    			total = total + 129;//what's the size of string/////////////////////////////////////////////////////////qqq
    		}
    	}
    	return total;
    }

    /**
     * Compares the specified object with this TupleDesc for equality.
     * Two TupleDescs are considered equal if they are the same size and if the
     * n-th type in this TupleDesc is equal to the n-th type in td.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
    	//your code here
    	if( o == this)
    	{
    		return true;
    	}
    	if(o == null)
    	{
    		return false;
    	}
    	if(o instanceof TupleDesc)
    	{
    		TupleDesc temp = (TupleDesc) o;
    		if(this.types.length != temp.types.length )
    		{
    			return false;
    		}
    		int size = this.types.length;
    		for(int i = 0; i < size; i++)
    		{
    			if(!(this.types[i]).equals(temp.types[i]))
    			{
    				return false;
    			}
    		}
    	}
    	return true;
    }
    

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * @return String describing this descriptor.
     */
    public String toString() {
        //your code here
    	StringBuilder sb = new StringBuilder();
    	for(int i =0; i < types.length;i++)
    	{
    		Type type = getType(i);
    		String name = getFieldName(i);
    		sb.append(type == Type.INT ? "INT" : "STRING");
    		sb.append('(');
    		sb.append(name);
    		if(i < types.length -1 )
    		{
    			sb.append("),");
    		}
    		else
    			sb.append(")");
    	}
    	return sb.toString();
    }
}
