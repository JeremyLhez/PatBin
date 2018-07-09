package fr.waves_rsp.patbin.query.continuous;

import java.util.ArrayList;
import java.util.List;

public class SPARQLAggregate {
	private List<String> values;
	// COUNT, MAX, MIN, SUM, AVG
	private String aggregator;

	public SPARQLAggregate(String aggregator) {
		this.aggregator = aggregator;
		values = new ArrayList<String>();
	}

	public void addValue(String value) {
		values.add(value);
	}

	public float getCurrentResult() {
		switch (aggregator) {
		case "COUNT":
			return count();
		case "MIN":
			return min();
		case "MAX":
			return max();
		case "SUM":
			return sum();
		case "AVG":
			return avg();
		default:
			throw new UnsupportedOperationException();
		}
	}

	private float count() {
		return values.size();
	}

	private float min() {
		float res = Float.parseFloat(values.get(0));

		for (int i = 1; i < values.size(); i++)
			if (Float.parseFloat(values.get(i)) < res)
				res = Float.parseFloat(values.get(i));

		return res;
	}

	private float max() {
		float res = Float.parseFloat(values.get(0));

		for (int i = 1; i < values.size(); i++)
			if (Float.parseFloat(values.get(i)) > res)
				res = Float.parseFloat(values.get(i));

		return res;
	}

	private float sum() {
		float res = 0.0f;

		for (String value : values)
			res = res + Float.parseFloat(value.split("\"")[1]);

		return res;
	}

	private float avg() {
		return sum() / values.size();
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();

		builder.append(aggregator + ": (");
		for (String f : values)
			builder.append(f + ", ");
		if (!values.isEmpty())
			builder.replace(builder.length() - 2, builder.length(), ")");
		else
			builder.replace(builder.length() - 1, builder.length(), "null");

		return builder.toString();
	}
}
