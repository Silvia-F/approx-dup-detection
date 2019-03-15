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

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.core.util.Utils;
import java.util.LinkedList;
import java.util.Vector;


/**
 * Describe your step plugin.
 * 
 */
public class ApproxDupDetection extends BaseStep implements StepInterface {
  
	private static Class<?> PKG = ApproxDupDetectionMeta.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$
	
	private ApproxDupDetectionData data;
	private ApproxDupDetectionMeta meta;
	
	protected int threshold;
  
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
			detectApproxDups(data.getGraph());
			System.out.println("QUEUE");
			for (int i = 0; i < data.getGraph().size(); i++) {
				System.out.println(data.getGraph().get(i).findSet());
			}
			
			setOutputDone();
			return false;
		}
		
		putRow( getInputRowMeta(), r ); // copy row to possible alternate rowset(s).
		
		if (first) {
			first = false;
		}
		
		if (!first) {
			String data_str = new String();
			for (int i = 0; i < getInputRowMeta().getFieldNames().length; i++) {
				data_str = data_str.concat(getInputRowMeta().getString(r, i));
				data_str = data_str.concat(" ");
			}
			data.addNode(data_str);
		}

		if ( checkFeedback( getLinesRead() ) ) {
			if ( log.isBasic() )
				logBasic( BaseMessages.getString( PKG, "ApproxDupDetection.Log.LineNumber" ) + getLinesRead() );
		}
      
		return true;
	}
	
	private void detectApproxDups(Vector<Node> graph) {
		double matchThreshold = 0.4;
		LinkedList<Node> queue = new LinkedList<Node>();
		graph.sort(null);		
		queue.addFirst(graph.get(0));;
		for (int i = 1; i < graph.size(); i++) {
			boolean changed = false;
			Node node = graph.get(i);
			for (int j = 0; j < queue.size(); j++) {
				Node queueNode = queue.get(j);
				if (node.findSet().equals(queueNode.findSet())) {
					queue.remove(j);
					queue.addFirst(node);
					changed = true;
					break;
				}
			}
			for (int j = 0; j < queue.size(); j++) {
				Node queueNode = queue.get(j);
				if (1 - ((double)Utils.getDamerauLevenshteinDistance(node.getData(), queueNode.getData()) /
						Math.max(node.getData().length(), queueNode.getData().length())) > matchThreshold) {
					node.union(queueNode);
					queue.remove(j);
					queue.addFirst(node);
					if (queue.size() > 4) {
						queue.removeLast();
					}
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
}