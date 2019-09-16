package org.pentaho.dataintegration;

import java.util.ArrayList;
import java.util.HashMap;

public class RecordGroup {
	private Double id;
	private ArrayList<Double> elements;
	private ArrayList<ArrayList<Double>> sims;
	public ArrayList<ArrayList<Double>> getSims() {
		return sims;
	}

	public void setSims(ArrayList<ArrayList<Double>> sims) {
		this.sims = sims;
	}

	private HashMap<Double, Double> outputSims;


	public HashMap<Double, Double> getOutputSims() {
		return outputSims;
	}

	public void setOutputSims(HashMap<Double, Double> outputSims) {
		this.outputSims = outputSims;
	}

	public RecordGroup(Double id) {
		this.id = id;
		this.elements = new ArrayList<Double > ();
		this.elements.add(id);
		this.sims = new ArrayList<ArrayList<Double>> ();
		this.outputSims = new HashMap<Double, Double> ();
	}
	
	public void addElement(ArrayList<Double> lst) {	
		if (!elements.contains(lst.get(0)))
			elements.add(lst.get(0));
		if (!elements.contains(lst.get(1)))
			elements.add(lst.get(1));
	}
	
	public double[] computeDistances(Double record) {
		double total = 0;
		double without = 0;
		for (ArrayList<Double> a: sims) {
			total += a.get(2);
			if (elements.size() > 2 && !a.get(0).equals(record) && !a.get(1).equals(record)) {
				without += a.get(2);
			}
		}
		total = total / (elements.size() * (elements.size() - 1));
		
		if (elements.size() > 2) {
			without = without / ((elements.size() - 1) * (elements.size() - 2));
			double difference = (total - without) / total;
			return new double[] { total, difference };
		}
		return new double[] { total };
	}
	
	public void calculateOutputSims() {		
		for (Double d: elements) {
			outputSims.put(d, new Double(0));
			int counter = 0;
			for (ArrayList<Double> lst: sims) {
				if ((lst.get(0).equals(d) && elements.contains(lst.get(1))) || (lst.get(1).equals(d) && elements.contains(lst.get(0)))) {
					outputSims.put(d, outputSims.get(d) + lst.get(2));
					counter++;
				}
			}
			outputSims.put(d, outputSims.get(d) / counter);
		}
	}
	
	public void removeRecord(Double record) {
		elements.remove(record);
		//partialSims.remove(record);
	}
	
	public void addSims(ArrayList<ArrayList<Double>> blockSims) {
		Double last = elements.get(elements.size() - 1);
		for (ArrayList<Double> lst: blockSims) {
			if (lst.get(0) > last)
				break;
			for (int i = 0; i < elements.size(); i++) {
				for (int j = i + 1; j < elements.size(); j++) {
					if (lst.get(0).equals(elements.get(i)) && lst.get(1).equals(elements.get(j))) {
						sims.add(lst);
					}
				}
			}
		}
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

}
