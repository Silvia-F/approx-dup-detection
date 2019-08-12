/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.pentaho.dataintegration;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;


public class DIDuplicateDetectionData extends BaseStepData implements StepDataInterface {

	private RowMetaInterface outputRowMeta;
	protected Vector<Node> graph; // Keeps nodes that form the graph for the domain-independent approach
	protected List<Object[]> buffer; // Keeps row data for output
	private int rowIndex; // Keeps the index of the last processed row
	
	/**
	 * 
	 */
	public DIDuplicateDetectionData() {
		super();
		graph = new Vector<Node>();
	buffer = new ArrayList<Object[]>( 5000 );
	rowIndex = 0;
	}
	
	public void setOutputRowMeta(RowMetaInterface outputRowMeta) {
		this.outputRowMeta = outputRowMeta;
	}
	
	public RowMetaInterface getOutputRowMeta() {
		return this.outputRowMeta;
	}
	
	/**
	 * Add node to the graph
	 */
	public void addNode(String data, int index) {
		graph.add(new Node(data, index));
	}
	
	public Vector<Node> getGraph() {
		return graph;
	}
	
	public void incrementIndex() {
		rowIndex++;
	}
	
	public int getIndex() {
		return rowIndex;
	}
}