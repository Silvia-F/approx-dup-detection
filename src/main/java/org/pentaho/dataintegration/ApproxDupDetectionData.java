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

import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 
 * This class holds temporary data for the approximate duplicate detection calculations
 * 
 */
public class ApproxDupDetectionData extends BaseStepData implements StepDataInterface {

	private RowMetaInterface outputRowMeta;
	protected List<Object[]> buffer; // Keeps row data for output
	private int rowIndex; // Keeps the index of the last processed row 
	
	// Blocks to partition records. The key is the value of grouping field and the list has record data needed for the calculations
	private Map<String, List<Object>> blocks; 

	
	/**
	* Create a new ApproxDupDetectionData instance
	*/
	public ApproxDupDetectionData() {
		super();
		buffer = new ArrayList<Object[]>( 5000 );
		
		blocks = new HashMap<String, List<Object>> ( 5000 );
		rowIndex = 0;
	}
	
	public void setOutputRowMeta(RowMetaInterface outputRowMeta) {
		this.outputRowMeta = outputRowMeta;
	}
	
	public RowMetaInterface getOutputRowMeta() {
		return this.outputRowMeta;
	}
	
	public void incrementIndex() {
		rowIndex++;
	}
	
	public int getIndex() {
		return rowIndex;
	}
	
	public Map<String, List<Object>> getBlocks() {
		return blocks;
	}
}