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

import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.core.util.Utils;

import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.Locale;
import java.util.Vector;


/**
 * Describe your step plugin.
 * 
 */
public class ApproxDupDetection extends BaseStep implements StepInterface {
  
	private static Class<?> PKG = ApproxDupDetectionMeta.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$
	
	private ApproxDupDetectionData data;
	private ApproxDupDetectionMeta meta;

  
	public ApproxDupDetection( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
			Trans trans ) {
		super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
	}
  
	/**
	* Initialize and do work where other steps need to wait for...
	*
	* @param stepMetaInterface
	*          The metadata to work with
	* @param stepDataInterface
	*          The data to initialize
	*/
	public boolean init( StepMetaInterface stepMetaInterface, StepDataInterface stepDataInterface ) {
		meta = (ApproxDupDetectionMeta) stepMetaInterface;
		data = (ApproxDupDetectionData) stepDataInterface;

		return super.init(stepMetaInterface, stepDataInterface);
	}

	public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {
		Object[] r = getRow(); // get row, set busy!
		if ( r == null ) {
			// no more input to be expected...
			if (meta.getMatchMethod().equals("Domain-Independent")) {
				detectDIApproxDups();
			}
			else {
				System.out.println("RULES");
			}
			
			writeOutput();
			
			setOutputDone();
			return false;
		}
			
		if (first) {
			data.setOutputRowMeta(getInputRowMeta().clone());
			data.getOutputRowMeta().addValueMeta(ValueMetaFactory.createValueMeta( meta.getColumnName(), ValueMetaInterface.TYPE_INTEGER ));
			data.getOutputRowMeta().addValueMeta(ValueMetaFactory.createValueMeta( "Similarity", ValueMetaInterface.TYPE_NUMBER ));
			System.out.println(data.getOutputRowMeta());
			first = false;
		}
		if (meta.getMatchMethod().equals("Domain-Independent")) {
			data.buffer.add(r);
			data.incrementIndex();
			String data_str = new String();			
			for (int i = 0; i < getInputRowMeta().getFieldNames().length; i++) {
				if (getInputRowMeta().getString(r, i) != null)
					data_str = data_str.concat(getInputRowMeta().getString(r, i));
				data_str = data_str.concat(" ");
			}
			data.addNode(data_str, data.getIndex());
			
			if ( checkFeedback( getLinesRead() ) ) {
				if ( log.isBasic() )
					logBasic( BaseMessages.getString( PKG, "ApproxDupDetection.Log.LineNumber" ) + getLinesRead() );
			}
		}
		else {
			System.out.println("RULE PREPROCESSING");
		}
		return true;
	}
	
	private void detectDIApproxDups() {
		double matchThreshold = meta.getMatchThreshold();
		LinkedList<Node> queue = new LinkedList<Node>();
		Vector<Node> orderedGraph = new Vector<Node> ();	
		orderedGraph.addAll(data.getGraph());
		orderedGraph.sort(null);
		queue.addFirst(orderedGraph.get(0));
		// First pass
		for (int i = 1; i < orderedGraph.size(); i++) {
			boolean changed = false;
			Node node = orderedGraph.get(i);
			
			for (int j = 0; j < queue.size(); j++) {
				Node queueNode = queue.get(j);
				
				if (1 - ((double)Utils.getDamerauLevenshteinDistance(node.getData(), queueNode.getData()) /
						Math.max(node.getData().length(), queueNode.getData().length())) > matchThreshold) {
					node.union(queueNode);
					queue.addFirst(queueNode);
					queue.remove(j + 1);
					changed = true;
					break;
				}				
			}
			if (!changed) {				
				queue.addFirst(node);
				if (queue.size() > 4) {
					queue.removeLast();
				}
			}			
		}	
		
		for (int i = 0; i < orderedGraph.size(); i++) {
			StringBuilder reversed = new StringBuilder();
			reversed.append(orderedGraph.get(i).getData());
			orderedGraph.get(i).setReversedData(reversed.reverse().toString());
		}
		Collections.sort(orderedGraph, new Comparator<Node>() {
			public int compare(Node e1, Node e2) {
				return e1.getReversedData().compareTo(e2.getReversedData());
			}});
		queue.clear();
		queue.addFirst(orderedGraph.get(0));
		// Second pass
		for (int i = 1; i < orderedGraph.size(); i++) {
			boolean changed = false;
			Node node = orderedGraph.get(i);
			for (int j = 0; j < queue.size(); j++) { // The set match verification is needed in the second pass
				Node queueNode = queue.get(j);
				if (node.findSet().equals(queueNode.findSet())) {
					queue.addFirst(queueNode);
					queue.remove(j + 1);
					changed = true;
					break;
				}
			}
			for (int j = 0; j < queue.size(); j++) {
				Node queueNode = queue.get(j);
				if (1 - ((double)Utils.getDamerauLevenshteinDistance(node.getReversedData(), queueNode.getReversedData()) /
						Math.max(node.getReversedData().length(), queueNode.getReversedData().length())) > matchThreshold) {
					node.union(queueNode);
					queue.addFirst(queueNode);
					queue.remove(j + 1);
					changed = true;
					break;
				}				
			}
			if (!changed) {		
				queue.addFirst(node);
				if (queue.size() > 4) {
					queue.removeLast();
				}
			}
		}	
	}
	
	private void writeOutput() throws KettleStepException, KettlePluginException {
		for (int i = 0; i < data.buffer.size(); i++) {
			Object[] newRow = new Object[data.buffer.get(i).length + 2];
			for (int j = 0; j < data.buffer.get(i).length; j++) 
				newRow[j] = data.buffer.get(i)[j];
			RowMeta rowMeta = new RowMeta();
			rowMeta.addValueMeta(ValueMetaFactory.createValueMeta( meta.getColumnName(), ValueMetaInterface.TYPE_INTEGER ));
			rowMeta.addValueMeta(ValueMetaFactory.createValueMeta( "Similarity", ValueMetaInterface.TYPE_NUMBER ));		
			
			 double similarity = (1 - ((double)Utils.getDamerauLevenshteinDistance(data.getGraph().get(i).findSet().getData(), data.getGraph().get(i).getData()) /
						Math.max(data.getGraph().get(i).findSet().getData().length(), data.getGraph().get(i).getData().length())));
			 DecimalFormatSymbols symbols = new DecimalFormatSymbols(Locale.getDefault());
			 symbols.setDecimalSeparator('.');
			 DecimalFormat df = new DecimalFormat("#.#", symbols);
			 
			 df.setRoundingMode(RoundingMode.DOWN);
			 similarity = Double.parseDouble(df.format(similarity));

					
			RowMetaAndData newRowMD = new RowMetaAndData(rowMeta, new Object[] { new Long( data.getGraph().get(i).findSet().getIndex()), similarity});
			newRow = RowDataUtil.addRowData( newRow, getInputRowMeta().size(), newRowMD.getData() );
			putRow( data.getOutputRowMeta(), newRow);
		}
	}
}