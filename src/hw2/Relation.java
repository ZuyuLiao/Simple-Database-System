package hw2;

import java.lang.reflect.Type;
import java.util.ArrayList;

import hw1.Field;
import hw1.RelationalOperator;
import hw1.Tuple;
import hw1.TupleDesc;

/**
 * This class provides methods to perform relational algebra operations. It will be used
 * to implement SQL queries.
 * @author Doug Shook
 *
 */
public class Relation {

	private ArrayList<Tuple> tuples;
	private TupleDesc td;
	
	public Relation(ArrayList<Tuple> l, TupleDesc td) {
		//your code here
		tuples = l;
		this.td = td;
	}
	
	/**
	 * This method performs a select operation on a relation
	 * @param field number (refer to TupleDesc) of the field to be compared, left side of comparison
	 * @param op the comparison operator
	 * @param operand a constant to be compared against the given column
	 * @return
	 */
	public Relation select(int field, RelationalOperator op, Field operand) {
		//your code here
		ArrayList<Tuple> ans = new ArrayList<Tuple>();
		for( Tuple i: tuples) {
			if(i.getField(field).compare(op, operand))
			{
				ans.add(i);
			}
		}
		return new Relation(ans, this.td);
	}
	
	/**
	 * This method performs a rename operation on a relation
	 * @param fields the field numbers (refer to TupleDesc) of the fields to be renamed
	 * @param names a list of new names. The order of these names is the same as the order of field numbers in the field list
	 * @return
	 */
	public Relation rename(ArrayList<Integer> fields, ArrayList<String> names) {
		//your code here
		hw1.Type[] t = new hw1.Type[td.numFields()];
		String[] s = new String[td.numFields()];
		for(int i =0; i <td.numFields();i++ )
		{
			t[i] = td.getType(i);
		}
		for(int j = 0; j < td.numFields();j++)
		{
			s[j] = td.getFieldName(j);
		}
		
		for(int k =0; k < fields.size();k++)
		{
			s[fields.get(k)] = names.get(k);
		}
		
		TupleDesc ans = new TupleDesc(t,s);
		
		
		return new Relation(this.tuples, ans);
	}
	
	/**
	 * This method performs a project operation on a relation
	 * @param fields a list of field numbers (refer to TupleDesc) that should be in the result
	 * @return
	 */
	public Relation project(ArrayList<Integer> fields) {
		//your code here
		hw1.Type[] t = new hw1.Type[fields.size()];
		String[] s = new String[fields.size()];
		for(int i = 0; i<fields.size();i++)
		{
			t[i] = this.td.getType(fields.get(i));
			s[i] = this.td.getFieldName(fields.get(i));
		}
		TupleDesc ntd = new TupleDesc(t,s);
		ArrayList<Tuple> ans = new ArrayList<Tuple>();
		for(int k =0; k < this.tuples.size();k++)
		{
		for(int j = 0; j < fields.size();j++)
		{
			Tuple nt = new Tuple(ntd);
			nt.setField(j, this.tuples.get(k).getField(fields.get(j)));
			ans.add(nt);
		}
		
		}
		return new Relation(ans, ntd);
	}
	
	/**
	 * This method performs a join between this relation and a second relation.
	 * The resulting relation will contain all of the columns from both of the given relations,
	 * joined using the equality operator (=)
	 * @param other the relation to be joined
	 * @param field1 the field number (refer to TupleDesc) from this relation to be used in the join condition
	 * @param field2 the field number (refer to TupleDesc) from other to be used in the join condition
	 * @return
	 * @throws Exception 
	 */
	public Relation join(Relation other, int field1, int field2) throws Exception {
		//your code here
		ArrayList<Tuple> ans = new ArrayList<Tuple>();
		hw1.Type[] t = new hw1.Type[this.td.numFields()+other.td.numFields()];
		String[] s = new String[this.td.numFields()+other.td.numFields()];
		int typeLength = 0;
		for(; typeLength < this.td.numFields(); typeLength++)
		{
			t[typeLength] = this.td.getType(typeLength);
			s[typeLength] = this.td.getFieldName(typeLength);
		}
		int sl = 0;
		for(; typeLength < this.td.numFields() + other.td.numFields(); typeLength++)
		{
			
			t[typeLength] = other.td.getType(sl);
			s[typeLength] = other.td.getFieldName(sl);
			sl++;
		}
		TupleDesc newtd = new TupleDesc(t,s);
		
		
		
		for(int i = 0; i < this.tuples.size();i++)
		{
			for(int j =0; j< other.tuples.size();j++) {
				Field aa = this.tuples.get(i).getField(field1);
				Field bb = other.tuples.get(j).getField(field2);
				if(aa.compare(RelationalOperator.EQ, bb))
				{
					Tuple newtuple = new Tuple(newtd);
					int k = 0;
					int k2=0;
					for(; k < this.td.numFields();k++)
					{
						newtuple.setField(k,this.tuples.get(i).getField(k));
					}
					for(;k<this.td.numFields()+other.td.numFields();k++)
					{
						for(;k2<other.td.numFields();k2++)
						{
							newtuple.setField(k, other.tuples.get(j).getField(k2));
						}
					}
					ans.add(newtuple);
				}
					
			}
		}
		return new Relation(ans, newtd);
	}
	
	/**
	 * Performs an aggregation operation on a relation. See the lab write up for details.
	 * @param op the aggregation operation to be performed
	 * @param groupBy whether or not a grouping should be performed
	 * @return
	 * @throws Exception 
	 */
	public Relation aggregate(AggregateOperator op, boolean groupBy) throws Exception {
		//your code here
		if(this.td.getType(0) == hw1.Type.STRING)
		{
			throw new Exception("string can not be aggregated");
		}
		Aggregator aggregator = new Aggregator(op, groupBy, this.td);
		for(Tuple t: this.tuples)
		{
			aggregator.merge(t);
		}
		return new Relation(aggregator.getResults(), this.td);
	}
	
	public TupleDesc getDesc() {
		//your code here
		return this.td;
	}
	
	public ArrayList<Tuple> getTuples() {
		//your code here
		return this.tuples;
	}
	
	/**
	 * Returns a string representation of this relation. The string representation should
	 * first contain the TupleDesc, followed by each of the tuples in this relation
	 */
	public String toString() {
		//your code here
		StringBuilder sb =new StringBuilder();
		sb.append(this.td.toString());
		for(Tuple t : this.tuples)
		{
			sb.append(t.toString());
		}
		return sb.toString();
	}
}
