package org.archive.petabox;

import java.io.IOException;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;

import org.mortbay.util.ajax.JSON;

/**
 * Petabox Item metadata.
 * Currently not designed for general use - Only useful for PetaboxFileSystem.
 * @contributer Kenji Nagahashi
 */
public class ItemMetadata {
	String server;
	String d1;
	String d2;
	String dir;
	String[] collection;
	Map<String, String> properties;
	long created;
	long updated;
	ItemFile[] files;
	boolean solo;
	//    public ItemMetadata(JSONObject jo) {
	//      this.server = jo.optString("server");
	//      this.d1 = jo.optString("d1");
	//      this.d2 = jo.optString("d2");
	//      this.created = Long.parseLong(jo.optString("created", "0"));
	//      this.updated = Long.parseLong(jo.optString("updated", "0"));
	//      this.properties = new HashMap<String, String>();
	//      JSONObject joprops = jo.optJSONObject("metadata");
	//      if (joprops != null) {
	//	for (String k : JSONObject.getNames(joprops)) {
	//	  // all metadata/* except for collection has string value.
	//	  if (k.equals("collection")) {
	//	    JSONArray colls = joprops.optJSONArray(k);
	//	    if (colls != null) {
	//	      collection = new String[colls.length()];
	//	      for (int i = 0; i < collection.length; i++) {
	//		collection[i] = colls.optString(i);
	//	      }
	//	    }
	//	  } else {
	//	    this.properties.put(k, joprops.optString(k));
	//	  }
	//	}
	//      }
	//      JSONArray jofiles = jo.optJSONArray("files");
	//      if (jofiles != null) {
	//	this.files = new ItemFile[jofiles.length()];
	//	for (int i = 0; i < jofiles.length(); i++) {
	//	  JSONObject jofile = jofiles.optJSONObject(i);
	//	  if (jofile != null) {
	//	    try {
	//	      this.files[i] = new ItemFile(jofile);
	//	    } catch (JSONException ex) {
	//	    }
	//	  }
	//	}
	//      }
	//    }
	final static String getString(Map<String, Object> map, String key) {
		Object o = map.get(key);
		return o != null ? o.toString() : null;
	}
	final static long parseLong(String o) {
		if (o == null || o.equals("")) return 0;
		try {
			return Long.parseLong(o);
		} catch (NumberFormatException ex) {
			return 0;
		}
	}
	final static long parseLong(Object o) {
		return parseLong(o != null ? o.toString() : null);
	}
	final static boolean getBoolean(Map<String, Object> map, String key, boolean defaultValue) {
		Object o = map.get(key);
		if (o instanceof Boolean) {
			return (Boolean)o;
		} else {
			return defaultValue;
		}
	}
	final static boolean getBoolean(Map<String, Object> map, String key) {
		return getBoolean(map, key, false);
	}

	@SuppressWarnings("unchecked")
	public ItemMetadata(Map<String, Object> jo) {
		// metadata API returns empty object ("{}") for non-existent item.
		// this can happen when item lookup is out of sync with actual system
		// (item deleted/lost, confused during shuffling, etc.). detect this
		// early and don't fail.
		if (jo.isEmpty()) return;

		this.server = getString(jo, "server");
		// for helping debug metadata API.
		if (this.server == null) {
			//	LOG.warn("jo=" + JSON.toString(jo));
		}
		this.d1 = getString(jo, "d1");
		this.d2 = getString(jo, "d2");
		this.created = parseLong(jo.get("created"));
		this.updated = parseLong(jo.get("updated"));
		this.solo = getBoolean(jo, "solo");
		this.properties = new HashMap<String, String>();
		Map<String, Object> joprops = (Map<String, Object>)jo.get("metadata");
		if (joprops != null) {
			for (String k : joprops.keySet()) {
				// all metadata/* except for collection has string value.
				if (k.equals("collection")) {
					// collection member is a string if there's only one collection.
					Object collection = joprops.get(k);
					if (collection instanceof String) {
						this.collection = new String[] { (String)collection };
					} else if (collection != null) {
						Object[] colls = (Object[])collection;
						if (colls != null) {
							this.collection = new String[colls.length];
							for (int i = 0; i < this.collection.length; i++) {
								this.collection[i] = colls[i].toString();
							}
						}
					}
				} else {
					this.properties.put(k, joprops.get(k).toString());
				}
			}
		}
		Object[] jofiles = (Object[])jo.get("files");
		if (jofiles != null) {
			this.files = new ItemFile[jofiles.length];
			for (int i = 0; i < jofiles.length; i++) {
				Map<String, Object> jofile = (Map<String, Object>)jofiles[i];
				if (jofile != null) {
					this.files[i] = new ItemFile(jofile);
				}
			}
		}
	}
	@SuppressWarnings("unchecked")
	public ItemMetadata(Reader reader) throws /*JSONException, */IOException {
		//this(new JSONObject(new JSONTokener(reader)));
		this((Map<String, Object>)JSON.parse(reader));
	}
	public boolean isCollection() {
		return properties != null && "collection".equals(properties.get("mediatype"));
	}

	public long getUpdated() {
		return updated;
	}
	public boolean isSolo() {
		return solo;
	}
	public ItemFile[] getFiles() {
		return files;
	}
	public String getD1() {
		return d1;
	}
	public String getD2() {
		return d2;
	}
	public String getServer() {
		return server;
	}

}