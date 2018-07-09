package fr.waves_rsp.patbin;

public class PatBin {
	private String pattern;
	private String binding;

	public PatBin(String pattern, String binding) {
		this.pattern = pattern;
		this.binding = binding;
	}

	public String getPattern() {
		return pattern;
	}

	public String getBinding() {
		return binding;
	}
	
	public void setBinding(String binding) {
		
	}

	@Override
	public String toString() {
		return "pattern: " + pattern + System.getProperty("line.separator") + "binding: " + binding;
	}
}
