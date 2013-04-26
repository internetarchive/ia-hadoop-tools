package org.archive.petabox;

public interface PetaboxClientConfig {
	public int getInt(String name, int defaultValue);
	public boolean getBoolean(String name, boolean defaultValue);
	public String getString(String name);
}