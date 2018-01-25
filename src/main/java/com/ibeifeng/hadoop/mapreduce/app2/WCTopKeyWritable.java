package com.ibeifeng.hadoop.mapreduce.app2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class WCTopKeyWritable implements WritableComparable<WCTopKeyWritable> {
	private String word;
	private int count;

	public WCTopKeyWritable() {
	}

	public WCTopKeyWritable(String word, int count) {
		this.set(word, count);
	}

	public void set(String word, int count) {
		this.word = word;
		this.count = count;
	}

	public String getWord() {
		return word;
	}

	public void setWord(String word) {
		this.word = word;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(word);
		out.writeInt(count);

	}

	public void readFields(DataInput in) throws IOException {
		this.word = in.readUTF();
		this.count = in.readInt();

	}

	public int compareTo(WCTopKeyWritable o) {

		return Integer.valueOf(this.count).compareTo(
				Integer.valueOf(o.getCount()));
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + count;
		result = prime * result + ((word == null) ? 0 : word.hashCode());
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
		WCTopKeyWritable other = (WCTopKeyWritable) obj;
		if (count != other.count)
			return false;
		if (word == null) {
			if (other.word != null)
				return false;
		} else if (!word.equals(other.word))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return word + "\t" + count;
	}
	
	
	
	
}
