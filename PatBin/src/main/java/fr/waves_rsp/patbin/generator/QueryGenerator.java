package fr.waves_rsp.patbin.generator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.jena.ext.com.google.common.collect.Iterables;

import fr.waves_rsp.patbin.PatBin;
import fr.waves_rsp.patbin.generator.util.PatternComparator;

public class QueryGenerator {
	private PatBin queryPatBin;
	private String streamPattern;
	private List<String> streamSplitBinding;
	private Map<Integer, String> variablesBranches;

	/**
	 * Basic constructor for the query generator. Uses a stream extract and an
	 * excel file (to be added later) from which data must be extracted to build
	 * the query itself.
	 * 
	 * @param pbs
	 */
	public QueryGenerator(PatBin pbs) {
		this.queryPatBin = null;
		this.streamPattern = pbs.getPattern();
		this.streamSplitBinding = new ArrayList<String>(Arrays.asList(pbs.getBinding().split(";")));
		this.variablesBranches = new HashMap<Integer, String>();
	}

	/**
	 * Builds the query, step by step.
	 */
	public void buildQuery() {
		this.init();

		// TODO temporary: need to build a function to identify the variables
		// and relevant objects, and to detect their position in the stream.
		Map<String, Integer> tempMap = new HashMap<String, Integer>();
		tempMap.put("var1", 6);
		tempMap.put("var2", 9);

		this.variablesBranches(tempMap);
		this.queryPatBin = this.buildWhereTree();
	}

	/**
	 * Initialization of the query: sets the current binding to something blank.
	 */
	private void init() {
		StringBuilder bindingBuilder = new StringBuilder(" ");

		// builds a blank binding, with the same number of objects than the
		// original one.
		for (int i = 1; i < streamSplitBinding.size(); i++)
			bindingBuilder.append("; ");

		this.streamSplitBinding = Arrays.asList(bindingBuilder.toString().split(";"));
	}

	/**
	 * Adds to the generator the variables and the objects that should be taken
	 * in consideration for building the query. It will affect the binding, and
	 * each variable will have its own branch (from the root of the pattern)
	 * stored. The branches will later be used to build the where clause.
	 * 
	 * @param variables
	 *            The variables and objects to be added to the binding of the
	 *            query, with their position in the binding for each.
	 */
	private void variablesBranches(Map<String, Integer> variables) {

		// for each variable
		for (String variable : variables.keySet()) {
			int position = variables.get(variable);
			// put the variable in the binding (basically it is blank)
			streamSplitBinding.set(position, variable);
			// build the branch associated to this variable (the branch is
			// identified by the variable's position)
			variablesBranches.put(position, buildBranchFromLeaf(position));
		}
	}

	/**
	 * Builds the WHERE clause of the query , using the branches stored to make
	 * a minimal tree linking all variables and objects.
	 * 
	 * @return A first version of PatBin corresponding to the query.
	 */
	public PatBin buildWhereTree() {
		Map<Integer, String> branches = new HashMap<Integer, String>(variablesBranches);

		// delete the common part of the beginning of each branch
		Map<Integer, String> commonRootBranches;
		boolean loop = true;
		while (loop) {
			char firstchar = Iterables.get(branches.values(), 0).charAt(0);

			// check if every branch has the same beginning
			for (String branch : branches.values())
				if (branch.charAt(0) != firstchar) {
					loop = false;
					break;
				}

			if (loop) {
				commonRootBranches = new HashMap<Integer, String>();
				for (int pos : branches.keySet()) {
					String branch = branches.get(pos);
					int end = branch.length();
					// adapting the parenthesis
					if (firstchar == '(')
						end = end - 1;
					commonRootBranches.put(pos, branch.substring(1, end));
				}
				branches = new HashMap<Integer, String>(commonRootBranches);
			}
		}

		// add the bindings for each branch
		Map<String, String> patternsBindings = new TreeMap<String, String>(new PatternComparator());
		for (int pos : branches.keySet()) {
			String currentBranch = branches.get(pos);
			StringBuilder builder = new StringBuilder();
			int elements = currentBranch.length() - currentBranch.replaceAll(":", "").length();

			for (int k = 0; k < elements; k++)
				builder.append(" ;");

			builder.append(streamSplitBinding.get(pos));
			patternsBindings.put(currentBranch, builder.toString().trim());
		}

		// combine all branches to build the common tree
		StringBuilder finalPattern = new StringBuilder();
		StringBuilder finalBinding = new StringBuilder();
		for (String key : patternsBindings.keySet()) {
			finalPattern.append(key + ":");
			finalBinding.append(patternsBindings.get(key) + "; ");
		}
		finalPattern.setLength(finalPattern.length() - 1);
		finalBinding.setLength(finalBinding.length() - 2);

		return new PatBin(finalPattern.toString(), finalBinding.toString());
	}

	/**
	 * Builds a complete pattern branch, from the root to a specific variable
	 * (the position of the variable is the same in the pattern and in the
	 * binding).
	 * 
	 * @param index
	 *            The position of the variable in the binding.
	 */
	private String buildBranchFromLeaf(int index) {
		StringBuilder branch = new StringBuilder();
		String[] predicatePatternIds = streamPattern.split(":");

		// remove the end of the pattern (after the variable) and the predicate
		// identifiers that do not generate branches
		branch.append(predicatePatternIds[index].replace("(", "").replace(")", ""));
		for (int i = index; i >= 1; i--) {
			if ((predicatePatternIds[i].endsWith(")")) && (i != index))
				branch.insert(0, ")");
			if (predicatePatternIds[i].startsWith("("))
				branch.insert(0, predicatePatternIds[i - 1].replace("(", "") + ":(");
		}

		// remove the empty branches
		while (branch.toString().contains("()"))
			branch = new StringBuilder(branch.toString().replaceAll("[0-9]+:\\(\\)", ""));

		// add the closing parenthesis at the end
		int countParenthesis = branch.toString().length() - branch.toString().replaceAll("\\(", "").length();
		for (int i = 0; i < countParenthesis; i++)
			branch.append(")");

		return branch.toString();
	}

	/**
	 * Getter for the generated query.
	 * 
	 * @return The compressed generated query
	 */
	public PatBin getGeneratedQuery() {
		return queryPatBin;
	}
}
