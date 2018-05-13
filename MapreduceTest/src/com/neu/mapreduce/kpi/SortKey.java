package com.neu.mapreduce.kpi;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class SortKey implements WritableComparable<SortKey> {
	
	private String kpiKey;
	
	private Integer kpiValue;
	
	public SortKey(){}
	
	public SortKey(String kpiKey, Integer kpiValue) {
		this.kpiKey = kpiKey;
		this.kpiValue = kpiValue;
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(kpiKey);
		out.writeInt(kpiValue);
	}

	public void readFields(DataInput in) throws IOException {
		this.kpiKey = in.readUTF();
		this.kpiValue = in.readInt();
	}

	public int compareTo(SortKey o) {
		return o.kpiValue - this.kpiValue;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((kpiKey == null) ? 0 : kpiKey.hashCode());
		result = prime * result
				+ ((kpiValue == null) ? 0 : kpiValue.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SortKey other = (SortKey) obj;
		if (kpiKey == null) {
			if (other.kpiKey != null)
				return false;
		} else if (!kpiKey.equals(other.kpiKey))
			return false;
		if (kpiValue == null) {
			if (other.kpiValue != null)
				return false;
		} else if (!kpiValue.equals(other.kpiValue))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "[" +  kpiKey + "\t" + kpiValue + "]";
	}

	public String getKpiKey() {
		return kpiKey;
	}

	public void setKpiKey(String kpiKey) {
		this.kpiKey = kpiKey;
	}

	public Integer getKpiValue() {
		return kpiValue;
	}

	public void setKpiValue(Integer kpiValue) {
		this.kpiValue = kpiValue;
	}
}
