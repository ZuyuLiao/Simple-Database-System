package hw2;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.parser.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.*;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.util.TablesNamesFinder;

public class Query {

	private String q;
	
	public Query(String q) {
		this.q = q;
	}
	
	public Relation execute()  {
		Statement statement = null;
		try {
			statement = CCJSqlParserUtil.parse(q);
		} catch (JSQLParserException e) {
			System.out.println("Unable to parse query");
			e.printStackTrace();
		}
		Select selectStatement = (Select) statement;
		PlainSelect sb = (PlainSelect)selectStatement.getSelectBody();
		
		//select items
		List<SelectItem> selectitems = sb.getSelectItems();
		List<String> str_items = new ArrayList<String>();
		if (selectitems != null) {
			for (int i = 0; i < selectitems.size(); i++) {
				str_items.add(selectitems.get(i).toString());
			}
		}
		
		//select table
		TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
		List<String> tableList = tablesNamesFinder
				.getTableList(selectStatement);
		
		
		//join
		List<Join> joinList = sb.getJoins();
		List<String> tablewithjoin = new ArrayList<String>();
		if (joinList != null) {
			for (int i = 0; i < joinList.size(); i++) {
				tablewithjoin.add(joinList.get(i).toString());
			}
		}

		//where
		Expression where_expression = sb.getWhere();
		String str = where_expression.toString();

		//group by
		List<Expression> GroupByColumnReferences = sb
				.getGroupByColumnReferences();
		List<String> str_groupby = new ArrayList<String>();
		if (GroupByColumnReferences != null) {
			for (int i = 0; i < GroupByColumnReferences.size(); i++) {
				str_groupby.add(GroupByColumnReferences.get(i).toString());
			}
		}
		
		
		
		
		
		//your code here
		return null;
		
	}
}
