package org.pentaho.dataintegration;

public class Node {
	
	private int rank;
	private Node parent;
	
	public Node() { // The constructor corresponds to the makeSet operation
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
}
