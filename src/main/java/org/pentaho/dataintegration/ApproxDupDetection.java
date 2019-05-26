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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.Vector;


/**
 * This step allows to detect approximate duplicate record groups.
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
				detectRuleApproxDups();
			}
			
			writeOutput();
			
			setOutputDone();
			return false;
		}
			
		if (first) {
			data.setOutputRowMeta(getInputRowMeta().clone());
			data.getOutputRowMeta().addValueMeta(ValueMetaFactory.createValueMeta( meta.getColumnName(), ValueMetaInterface.TYPE_INTEGER ));
			data.getOutputRowMeta().addValueMeta(ValueMetaFactory.createValueMeta( "Similarity", ValueMetaInterface.TYPE_NUMBER ));
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
			data.incrementIndex();
			System.out.println("ON RULES");
			String[][] matching = meta.getMatching();
			float maxWeight = 0;
			String maxField = "";
			for (int i = 0; i < matching.length; i++) {			
				if (Float.parseFloat(matching[i][2]) > maxWeight) {
					maxWeight = Float.parseFloat(matching[i][2]);
					maxField = matching[i][0];
				}
			}
			for (int i = 0; i < getInputRowMeta().getFieldNames().length; i++) {
				if (getInputRowMeta().getFieldNames()[i].equals(maxField)) {
					System.out.println("Going for field " + maxField);
					double threshold= 0.4;
					boolean found = false;
					if (data.getBlocks().size() > 0) {
						String maxBlock = null;
						double maxSim = 0;
						for (int j = 0; j < data.getBlocks().size(); j++) {
							double similarity = 1 - ((double)Utils.getDamerauLevenshteinDistance(data.getBlocks().keySet().toArray()[j].toString(),
									getInputRowMeta().getString(r, i)) /
									Math.max(data.getBlocks().keySet().toArray()[j].toString().length(), getInputRowMeta().getString(r, i).length()));
							if (similarity > maxSim) {
								maxSim = similarity;
								maxBlock = data.getBlocks().keySet().toArray()[j].toString();
							}
						}
						if (maxSim > threshold) {
							found = true;
							data.getBlocks().get(maxBlock).add(data.getIndex());
							ArrayList<String> fields = new ArrayList<String> ();
							for (int k = 0; k < meta.getMatching().length; k++) {
								for (int l = 0; l < getInputRowMeta().getFieldNames().length; l++) {
									if (meta.getMatching()[k][0].equals(getInputRowMeta().getFieldNames()[l])) {
										fields.add(getInputRowMeta().getString(r, l));
										break;
									}
								}
							}
							System.out.println("FIELDS");
							System.out.println(fields);
							data.getBlocks().get(maxBlock).add(fields);
						}
					}
					if (!found) {
						data.getBlocks().put(getInputRowMeta().getString(r, i), new ArrayList<Object> ());
						data.getBlocks().get(getInputRowMeta().getString(r, i)).add(data.getIndex());
						ArrayList<String> fields = new ArrayList<String> ();
						for (int k = 0; k < meta.getMatching().length; k++) {
							for (int l = 0; l < getInputRowMeta().getFieldNames().length; l++) {
								if (meta.getMatching()[k][0].equals(getInputRowMeta().getFieldNames()[l])) {
									fields.add(getInputRowMeta().getString(r, l));
									break;
								}
							}
						}
						data.getBlocks().get(getInputRowMeta().getString(r, i)).add(fields);
					}
					break;
				}
			}
			System.out.println("PRINTING");
			System.out.println(data.getBlocks().size());
			System.out.println(data.getBlocks());
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
	@SuppressWarnings("unchecked")
	private void detectRuleApproxDups() {
		String[][] matching = meta.getMatching();
		Set<String> keys = data.getBlocks().keySet();
		for (String s: keys) {			
			List<String> field_data = (List<String>) data.getBlocks().get(s).get(1); // Data of the block representative
			for (int i = 1; i < data.getBlocks().get(s).size(); i = i + 2) {
				for (int j = i; j < data.getBlocks().get(s).size(); j = j + 2) {
					System.out.println("I: " + i + "; J: " + j);
					Object test = data.getBlocks().get(s).get(j);
					ArrayList<String> a = (ArrayList<String>)test;
					System.out.println("TRYING");
					System.out.println(a.get(0));
					System.out.println(a.get(1));
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