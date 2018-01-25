package com.ibeifeng.hadoop.mapreduce.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TestWritable implements WritableComparable<TestWritable>{
	
	// long ->LongWritable
	
	private LongWritable id;
	
	//String-> Text
    private Text name;
    
	public TestWritable() {
		id = new LongWritable();
		name = new Text();
		
	}
	
	public TestWritable(LongWritable id, Text name) {
		this.set(id, name);
	}

	public void set(LongWritable id, Text name) {
		this.id = id;
		this.name = name;
		
	}
	
	
	//get ---set 
	public LongWritable getId() {
		return id;
	}

	public void setId(LongWritable id) {
		this.id = id;
	}

	public Text getName() {
		return name;
	}

	public void setName(Text name) {
		this.name = name;
	}
	
	//
	public void write(DataOutput out) throws IOException {
		this.id.write(out);
		this.name.write(out);
		
	}

	public void readFields(DataInput in) throws IOException {
		this.id.readFields(in);
		this.id.readFields(in);
		
	}

	public int compareTo(TestWritable o) {
		int comp = this.id.compareTo(o.id);
		if (0 != comp){
			return comp;
		}
		return this.name.compareTo(o.name);
	}

	
	
	
	
	//toString()
	@Override
	public String toString() {
		return  id + "\t" + name;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		result = prime * result + ((name == null) ? 0 : name.hashCode());
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
		TestWritable other = (TestWritable) obj;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		return true;
	}
	
	
}
