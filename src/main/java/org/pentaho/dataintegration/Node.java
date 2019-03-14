package org.pentaho.dataintegration;

public class Node implements Comparable<Node> {
	
	private String data;
	private int rank;
	private Node parent;
	
	public Node(String data) { // The constructor corresponds to the makeSet operation
		this.data = data;
		this.parent = this;
		this.rank = 0;
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
}
