package org.archive.petabox;

import java.util.Map;

public class ItemFile {
	String name;
	String format;
	long mtime;
	long size;
	String md5;
	String crc32;
	String sha1;
	//    public ItemFile(JSONObject jo) throws JSONException {
	//      this.name = jo.getString("name");
	//      this.format = jo.optString("format");
	//      // these fields are returned as string.
	//      this.mtime = Long.parseLong(jo.optString("mtime", "0"));
	//      this.size = Long.parseLong(jo.optString("size", "0"));
	//      this.md5 = jo.optString("md5");
	//      this.crc32 = jo.optString("crc32");
	//      this.sha1 = jo.optString("sha1");
	//    }
	public ItemFile(Map<String, Object> jo) {
		this.name = (String)jo.get("name");
		this.format = (String)jo.get("format");
		// Metadata API returns these numeric values as strings
		this.mtime = ItemMetadata.parseLong(jo.get("mtime"));
		this.size = ItemMetadata.parseLong(jo.get("size"));
		this.md5 = (String)jo.get("md5");
		this.crc32 = (String)jo.get("crc32");
		this.sha1 = (String)jo.get("sha1");
	}

	public String getName() {
		return name;
	}
	public String getFormat() {
		return format;
	}
	public long getMtime() {
		return mtime;
	}
	public long getMtimeMS() {
		return mtime * 1000;
	}
	public long getSize() {
		return size;
	}
	public String getMd5() {
		return md5;
	}
	public String getCrc32() {
		return crc32;
	}
	public String getSha1() {
		return sha1;
	}


}