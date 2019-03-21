package org.pentaho.dataintegration;

public class Node implements Comparable<Node> {
	
	private String data;
	private String reversedData;
	private int rank;
	private int index;
	private Node parent;
	
	public Node(String data, int index) {
		this.data = data;
		this.reversedData = "";
		this.parent = this;
		this.rank = 0;
		this.index = index;
	}
	
	public Node makeSet() {
		this.parent = this;
		this.rank = 0;
		return this;
	}
	
	public Node findSet() {
		if (!this.equals(this.parent)) {
			this.parent = this.parent.findSet();
		}
		return this.parent;
	}
	
	public void link(Node node) {
		Node x = this.findSet();
		Node y = node.findSet();
		if (x.rank > y.rank) {
			y.parent = x;
		}
		else {
			x.parent = y;
			if (x.rank == y.rank) {
				y.rank++;
			}
		}
	}
	
	public void union(Node node) {
		this.link(node);
	}	
	
	public String getData() {
		return this.data;
	}
	
	public int compareTo(Node n2) {
		return this.getData().compareTo(n2.getData());
	}
	
	public void setReversedData(String data) {
		this.reversedData = data;
	}
	
	public String getReversedData() {
		return this.reversedData;
	}
	
	//DEBUG
	public String toString() {
		return this.getData();
	}
	
	public int getIndex() {
		return this.index;
	}
	
	public void setIndex(int index) {
		this.index = index;
	}	
}
