package fr.waves_rsp.patbin.query;

import java.util.ArrayList;
import java.util.List;

/**
 * Representation and storing of the aggregates.
 * 
 * @author Jeremy Lhez
 *
 */
public class AggregatesStorage {
	private List<String> aggregates;
	private String aggregate;
	private String having;
	private float result;

	public AggregatesStorage(String a) {
		this.aggregates = new ArrayList<String>();
		this.aggregate = a;
		this.having = null;
	}

	public AggregatesStorage(String a, String h) {
		this.aggregates = new ArrayList<String>();
		this.aggregate = a;
		this.having = h;
	}

	public void addAggregateValue(String value) {
		this.aggregates.add(value);
	}

	public float getResult() {
		return result;
	}

	/**
	 * Process the aggregates gathered. Calculates the result according to the
	 * specific aggregate, and verifies if it is valid according to the having
	 * clause. The result must be recovered using the get method.
	 * 
	 * @return true if the aggregate is valid, false otherwise.
	 */
	public boolean process() {
		if (aggregates.isEmpty()) {
			return false;
		}

		if (aggregate.equals("SUM")) {
			this.result = Float.parseFloat(aggregates.get(0));
			for (int i = 1; i < aggregates.size(); i++) {
				this.result += Float.parseFloat(aggregates.get(i));
			}
		} else if (aggregate.equals("MIN")) {
			this.result = Float.parseFloat(aggregates.get(0));
			float tmp;
			for (int i = 1; i < aggregates.size(); i++) {
				tmp = Float.parseFloat(aggregates.get(i));
				if (tmp < this.result)
					this.result = tmp;
			}
		} else if (aggregate.equals("MAX")) {
			this.result = Float.parseFloat(aggregates.get(0));
			float tmp;
			for (int i = 1; i < aggregates.size(); i++) {
				tmp = Float.parseFloat(aggregates.get(i));
				if (tmp > this.result)
					this.result = tmp;
			}
		} else if (aggregate.equals("AVG")) {
			result = Float.parseFloat(aggregates.get(0));
			for (int i = 1; i < aggregates.size(); i++) {
				result += Float.parseFloat(aggregates.get(i));
			}
			result = result / aggregates.size();
		} else if (aggregate.equals("COUNT")) {
			this.result = aggregates.size();
		}

		return evaluate();
	}

	/**
	 * Evaluates if the result processed is valid according to the having
	 * clause. If the having clause is null, then the result is automatically
	 * valid.
	 * 
	 * @return true if the result is valid, false otherwise.
	 */
	private boolean evaluate() {
		if (having == null) {
			return true;
		} else {
			if (having.startsWith("<=")) {
				if (result <= Float.parseFloat(having.substring(2)))
					return true;
				else
					return false;
			} else if (having.startsWith("<")) {
				if (result < Float.parseFloat(having.substring(1)))
					return true;
				else
					return false;
			} else if (having.startsWith(">=")) {
				if (result >= Float.parseFloat(having.substring(2)))
					return true;
				else
					return false;
			} else if (having.startsWith(">")) {
				if (result > Float.parseFloat(having.substring(1)))
					return true;
				else
					return false;
			} else if (having.startsWith("=")) {
				if (result == Float.parseFloat(having.substring(1)))
					return true;
				else
					return false;
			}
		}
		return false;
	}

	@Override
	public String toString() {
		return aggregate + having;
	}
}
