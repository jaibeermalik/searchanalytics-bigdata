package org.jai.search.model;

public class Specification {
	private String resolution;

	private String memory;

	public Specification(final String resoluton, final String memory) {
		this.resolution = resoluton;
		this.memory = memory;
	}

	public String getResolution() {
		return resolution;
	}

	public void setResolution(final String resolution) {
		this.resolution = resolution;
	}

	public String getMemory() {
		return memory;
	}

	public void setMemory(final String memory) {
		this.memory = memory;
	}
}
