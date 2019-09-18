package org.pentaho.dataintegration;

import java.util.ArrayList;
import java.util.HashMap;

public class RecordGroup {
	private Double id;
	private ArrayList<Double> elements;
	private ArrayList<ArrayList<Double>> sims;
	
	public RecordGroup(Double id) {
		this.id = id;
		this.elements = new ArrayList<Double> ();
		this.elements.add(id);
		this.sims = new ArrayList<ArrayList<Double>> ();
	}
	
	public void addElement(ArrayList<Double> lst) {
		if (!elements.contains(lst.get(1)))
			elements.add(lst.get(1));
		sims.add(lst);
	}
	
	public Double getId() {
		return id;
	}
	
	public void setId(Double id) {
		this.id = id;
	}
	
	public ArrayList<Double> getElements() {
		return elements;
	}
	
	public void setElements(ArrayList<Double> elements) {
		this.elements = elements;
	}
	
	public ArrayList<ArrayList<Double>> getSims() {
		return sims;
	}
	
	@Override
	public String toString() {
		return elements.toString();
	}
}
