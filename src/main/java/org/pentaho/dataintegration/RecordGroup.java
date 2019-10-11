package org.pentaho.dataintegration;

import java.util.ArrayList;
import java.util.HashMap;

public class RecordGroup {
	private Double id;
	private ArrayList<Double> elements;
	private ArrayList<ArrayList<Double>> sims;
	private double totalSim; 
	
	public RecordGroup(Double id) {
		this.id = id;
		this.elements = new ArrayList<Double> ();
		this.elements.add(id);
		this.sims = new ArrayList<ArrayList<Double>> ();
		totalSim = 0;
	}
	
	public void addElement(ArrayList<Double> lst) {
		if (!elements.contains(lst.get(1)))
			elements.add(lst.get(1));
	}
	
	public void addSims(ArrayList<ArrayList<Double>> block) {
		for (ArrayList<Double> lst: block) {
			if (elements.contains(lst.get(0)) && elements.contains(lst.get(1))) {
				sims.add(lst);
				totalSim += lst.get(2);
			}
		}
	}
	
	public double[] computeDistances(Double elem) {		
		double totalDistance = totalSim / (elements.size() * (elements.size() - 1));
		if (elements.size() < 3) {
			return new double[] { totalDistance };
		}
		double partialDistance = 0;
		double counter = 0;
		for (ArrayList<Double> lst: sims) {
			if (lst.get(0).equals(elem) || lst.get(1).equals(elem)) {
				partialDistance += lst.get(2);
				counter++;
			}
		}		
		partialDistance = partialDistance / counter;
		return new double[] { totalDistance, partialDistance };
	}

	public void removeElement(Double elem) {
		elements.remove(elem);
		for (int i = 0; i < sims.size(); i++) {
			if (sims.get(i).get(0).equals(elem) || sims.get(i).get(1).equals(elem)) {
				totalSim -= sims.get(i).get(2);
				sims.remove(i);
				i--;
			}
		}
	}

	public Double getOutputSim() {
		Double output = new Double(0);
		for (ArrayList<Double> lst: sims) {
			output += lst.get(2);		
		}
		return output / sims.size();
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
