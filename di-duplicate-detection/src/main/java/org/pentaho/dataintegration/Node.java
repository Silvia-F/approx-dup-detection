package org.pentaho.dataintegration;

import java.util.ArrayList;

public class Node implements Comparable<Node> {
	
	private String data; // The row data concatenated into a single String
	private String reversedData; // The data String, but the characters in reverse order
	private int index; // Index of the row in the dataset
	private Node parent; // The parent Node of the instance
	private ArrayList<Node> children; // List of node's children to facilitate future comparisons
	
	public Node(String data, int index) {
		this.data = data;
		StringBuilder reversed = new StringBuilder();
		reversed.append(data);
		this.reversedData = reversed.reverse().toString();
		this.parent = this;
		this.index = index;
		this.children = new ArrayList<Node> ();
	}
	
	/**
	 * Method to obtain the representative of the group, that corresponds to the parent Node
	 * @return Node corresponding to the representative Node
	 */
	public Node findSet() {
		if (!this.equals(this.parent)) {
			this.parent = this.parent.findSet();
		}
		return this.parent;
	}
	
	/**
	 * Method to merge two groups of Nodes
	 * @param node corresponding to a Node belong to another group to be merged
	 * @return Node corresponding to the representative of the new group
	 */	
	public Node union(Node node) {
		Node x = this.findSet();
		Node y = node.findSet();
		if (x.index > y.index) {
			x.parent = y;
			y.children.add(x);
			for (int i = 0; i < x.children.size(); i++) {
				if (! y.children.contains(x.children.get(i))) {
					x.children.get(i).parent = y;
					y.children.add(x.children.get(i));
				}
			}
			return y;
		}
		else {
			y.parent = x;
			x.children.add(y);
			for (int i = 0; i < y.children.size(); i++) {
				if (!x.children.contains(y.children.get(i))) {
					y.children.get(i).parent = x;
					x.children.add(y.children.get(i));
				}
			}
			return x;
		}
	}	
	
	/**
	 * Comparator used to order the Nodes based on their data's lexicographic order
	 */
	public int compareTo(Node n2) {
		return this.getData().compareTo(n2.getData());
	}
	
	public String getData() {
		return this.data;
	}
	
	public String getReversedData() {
		return this.reversedData;
	}
	
	public String toString() {
		return this.getData();
	}
	
	public int getIndex() {
		return this.index;
	}	
	
	public ArrayList<Node> getChildren() {
		return children;
	}
}
