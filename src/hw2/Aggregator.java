package hw2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import hw1.Field;
import hw1.IntField;
import hw1.Tuple;
import hw1.TupleDesc;

/**
 * A class to perform various aggregations, by accepting one tuple at a time
 * @author Doug Shook
 *
 */
public class Aggregator {
	
	AggregateOperator op;
	boolean isGroupBy;
	TupleDesc tupleDesc;
	ArrayList<Tuple> ans;
	HashMap<Field, Integer> values;
	HashMap<Field, Integer> counts;
	static final Field NO_GROUPPING_FIELD = new IntField(Integer.MAX_VALUE);

	public Aggregator(AggregateOperator o, boolean groupBy, TupleDesc td) {
		//your code here
		this.op = o;
		this.isGroupBy = groupBy;
		this.tupleDesc = td;
		this.ans = new ArrayList<Tuple>();
		this.values = new HashMap<Field, Integer>();
		this.counts = new HashMap<Field, Integer>();
	}

	/**
	 * Merges the given tuple into the current aggregation
	 * @param t the tuple to be aggregated
	 */
	public void merge(Tuple t) {
		//your code here
		
		if(!this.isGroupBy)
		{
			int rawValue = ((IntField) t.getField(0)).getValue();
			if(!counts.containsKey(NO_GROUPPING_FIELD))
			{
				values.put(Aggregator.NO_GROUPPING_FIELD, rawValue);
				counts.put(Aggregator.NO_GROUPPING_FIELD, 1);
			}
			else
			{
				values.put(Aggregator.NO_GROUPPING_FIELD, aggr(values.get(NO_GROUPPING_FIELD),rawValue));
				counts.put(Aggregator.NO_GROUPPING_FIELD, counts.get(NO_GROUPPING_FIELD)+1);
			}
		}
		else {
			int rawValue = ((IntField) t.getField(1)).getValue();
			if(!counts.containsKey(t.getField(0)))
			{
				values.put(t.getField(0), rawValue);
				counts.put(t.getField(0), 1);
			}
			else
			{
				values.put(t.getField(0), aggr(values.get(t.getField(0)),rawValue));
				counts.put(t.getField(0), counts.get(t.getField(0))+1);
			}
		}
	}
	
	
	private int aggr(int a, int b) {
        switch (op) {
            case COUNT: case AVG: case SUM:
                return a + b;
            case MAX:
                return Math.max(a, b);
            case MIN:
                return Math.min(a, b);
            default: return Integer.MAX_VALUE;
        }
    }
	
	/**
	 * Returns the result of the aggregation
	 * @return a list containing the tuples after aggregation
	 */
	public ArrayList<Tuple> getResults() {
		//your code here
		for(Field field : counts.keySet())
		{
			int val = values.get(field);
			int count = counts.get(field);
			Tuple tp = new Tuple(tupleDesc);
			if(tupleDesc.numFields() == 1)
			{
				switch (this.op) {
                case COUNT:
                    tp.setField(0, new IntField(count));
                    break;
                case AVG:
                    tp.setField(0, new IntField(val / count));
                    break;
                default:
                    tp.setField(0, new IntField(val));
                    break;
				}
			}
			else
			{
				tp.setField(0, field);
                switch (this.op) {
                    case COUNT:
                        tp.setField(1, new IntField(count));
                        break;
                    case AVG:
                        tp.setField(1, new IntField(val / count));
                        break;
                    default:
                        tp.setField(1, new IntField(val));
                        break;
                }
			}
			ans.add(tp);
		}
		
		return ans;
	}

}
