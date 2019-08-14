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
import com.wcohen.ss.Jaro;
import com.wcohen.ss.JaroWinkler;
import com.wcohen.ss.NeedlemanWunsch;

import org.apache.commons.codec.language.DoubleMetaphone;
import org.apache.commons.codec.language.Metaphone;
import org.apache.commons.codec.language.RefinedSoundex;
import org.apache.commons.codec.language.Soundex;
import org.apache.commons.lang.StringUtils;
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
import org.pentaho.di.trans.steps.fuzzymatch.LetterPairSimilarity;
import org.pentaho.di.core.util.Utils;

import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;


/**
 * This step allows to detect approximate duplicate record groups.
 * 
 */
public class ApproxDupDetection extends BaseStep implements StepInterface {
  
	private static Class<?> PKG = ApproxDupDetectionMeta.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$
	
	private ApproxDupDetectionData data;
	private ApproxDupDetectionMeta meta;
	private HashMap<Double, ArrayList<Double>> transitive;
  
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
			detectApproxDups();		
			writeOutput();	
			setOutputDone();
			return false;
		}
			
		if (first) {	
			
			data.setOutputRowMeta(getInputRowMeta().clone());
			meta.getFields(data.getOutputRowMeta(), getStepname(), null, null, this, repository, metaStore);

			// Normalize weights
			double total = 0;
			for (double[] measure: meta.getMeasures()) {
				total += measure[1];
			}
			for (int i = 0; i < meta.getMeasures().length; i++) {
				meta.getConvertedMeasures()[i][0] = meta.getMeasures()[i][0];
				meta.getConvertedMeasures()[i][1] = meta.getMeasures()[i][1] / total;
			}
			
			// Preapre for cartesian product
			data.getBlocks().put("", new ArrayList<Object>() );
			
			first = false;
		}
		
		data.buffer.add(r);
		data.incrementIndex();
		// Cartesian Product
		if (meta.getBlockingAttributes().size() == 0) {
			ArrayList<String> fields = new ArrayList<String> ();
			for (int j = 0; j < meta.getMatchFields().size(); j++) {
				for (int k = 0; k < getInputRowMeta().getFieldNames().length; k++) {
					if (meta.getMatchFields().get(j) != null && meta.getMatchFields().get(j).equals(getInputRowMeta().getFieldNames()[k])) {
						fields.add(getInputRowMeta().getString(r, k));
						break;
					}
				}
			}
			data.getBlocks().get("").add(data.getIndex());
			data.getBlocks().get("").add(fields);
		}	
		// Blocking
		else {
			String blockingValue = "";
			for (int i = 0; i < meta.getBlockingAttributes().size(); i++) {
				for (int j = 0; j < getInputRowMeta().getFieldNames().length; j++) {
					if (getInputRowMeta().getFieldNames()[j].equals(meta.getBlockingAttributes().get(i))) {
						blockingValue = blockingValue.concat(getInputRowMeta().getString(r, j));
					}
				}
			}
			ArrayList<String> fields = new ArrayList<String> ();
			for (int j = 0; j < meta.getMatchFields().size(); j++) {
				for (int k = 0; k < getInputRowMeta().getFieldNames().length; k++) {
					if (meta.getMatchFields().get(j) != null && meta.getMatchFields().get(j).equals(getInputRowMeta().getFieldNames()[k])) {
						fields.add(getInputRowMeta().getString(r, k));
						break;
					}
				}
			}				
				
			if (data.getBlocks().containsKey(blockingValue)) {
				data.getBlocks().get(blockingValue).add(data.getIndex());
				data.getBlocks().get(blockingValue).add(fields);
			}
			else {
				data.getBlocks().put(blockingValue, new ArrayList<Object> ());
				data.getBlocks().get(blockingValue).add(data.getIndex());
				data.getBlocks().get(blockingValue).add(fields);
			}
		}					
		if ( checkFeedback( getLinesRead() ) ) {
			if ( log.isBasic() )
				logBasic( BaseMessages.getString( PKG, "ApproxDupDetection.Log.LineNumber" ) + getLinesRead() );
		}
		return true;
	}
	
	@SuppressWarnings("unchecked")
	private void detectApproxDups() {
		ArrayList<ArrayList<ArrayList<Double>>> blockSims = new ArrayList<ArrayList<ArrayList<Double>>> ();
		Set<String> keys = data.getBlocks().keySet();
		for (String s: keys) {
			ArrayList<ArrayList<Double>> blockSim = new ArrayList<ArrayList<Double>>();
			for (int i = 1; i < data.getBlocks().get(s).size(); i +=  2) {
				ArrayList<String> a = (ArrayList<String>)data.getBlocks().get(s).get(i);
				for (int j = i + 2; j < data.getBlocks().get(s).size(); j += 2) {
					ArrayList<String> b = (ArrayList<String>)data.getBlocks().get(s).get(j);
					double similarity = 0;
					for (int k = 0; k < meta.getConvertedMeasures().length; k++) {
						switch ((int)meta.getConvertedMeasures()[k][0]) {
							case(0):								
								similarity += meta.getConvertedMeasures()[k][1] * (1 - StringUtils.getLevenshteinDistance( a.get(k), b.get(k)) / 
										(double)Math.max(a.get(k).length(), b.get(k).length()));
								break;
							case(1):
								similarity += meta.getConvertedMeasures()[k][1] * (1 - Utils.getDamerauLevenshteinDistance(a.get(k), b.get(k)) /
										(double)Math.max(a.get(k).length(), b.get(k).length()));
								break;
							case(2):								
								similarity += meta.getConvertedMeasures()[k][1] * (1 - Math.abs(new NeedlemanWunsch().score(a.get(k), b.get(k))) /
										(double)Math.max(a.get(k).length(), b.get(k).length()));
								break;
							case(3):
								similarity += meta.getConvertedMeasures()[k][1] * new Jaro().score(a.get(k), b.get(k));
								break;
							case(4):
								similarity += meta.getConvertedMeasures()[k][1] * new JaroWinkler().score(a.get(k), b.get(k));
								break;
							case(5):
								similarity += meta.getConvertedMeasures()[k][1] * LetterPairSimilarity.getSimiliarity(a.get(k), b.get(k));
								break;
							case(6):
								String metaphone1 = (new Metaphone()).metaphone( a.get(k) );
								String metaphone2 = (new Metaphone()).metaphone( b.get(k) );
								if (metaphone1.equals(metaphone2))
									similarity += meta.getConvertedMeasures()[k][1];
								break;
							case(7):
								String doubleMetaphone1 = (new DoubleMetaphone()).doubleMetaphone( a.get(k) );
								String doubleMetaphone2 = (new DoubleMetaphone()).doubleMetaphone( b.get(k) );
								if (doubleMetaphone1.equals(doubleMetaphone2))
									similarity += meta.getConvertedMeasures()[k][1];
								break;
							case(8):
								String soundex1 = (new Soundex()).encode( a.get(k) );
								String soundex2 = (new Soundex()).encode( b.get(k) );
								if (soundex1.equals(soundex2))
									similarity += meta.getConvertedMeasures()[k][1];
								break;
							case(9):
								String refSoundex1 = (new RefinedSoundex()).encode( a.get(k) );
								String refSoundex2 = (new RefinedSoundex()).encode( b.get(k) );
								if (refSoundex1.equals(refSoundex2))
									similarity += meta.getConvertedMeasures()[k][1];
								break;
						}
					}
					ArrayList<Double> temp = new ArrayList<Double>();
					temp.add(((Integer)data.getBlocks().get(s).get(i - 1)).doubleValue());
					temp.add(((Integer)data.getBlocks().get(s).get(j - 1)).doubleValue());
					temp.add(similarity);
					blockSim.add(temp);
				}
			}				
			blockSims.add(blockSim);
		}
		transitiveClosure(blockSims);
		
	}

	private void transitiveClosure(ArrayList<ArrayList<ArrayList<Double>>> blockSims) {
		transitive = new HashMap<Double, ArrayList<Double>> ();
		System.out.println(blockSims);
		for (int i  = 0; i < blockSims.size(); i++) {
			if (blockSims.get(i).size() == 0)
				continue;
			Set<Double> thisBlock = new HashSet<Double> ();
			for (int j = 0; j < blockSims.get(i).size(); j++) {	
				if (blockSims.get(i).get(j).get(2) >= meta.getMatchingThreshold()) {
					if (transitive.containsKey(blockSims.get(i).get(j).get(0))) {
						thisBlock.add(blockSims.get(i).get(j).get(1));
						transitive.get(blockSims.get(i).get(j).get(0)).add(blockSims.get(i).get(j).get(1));
						transitive.get(blockSims.get(i).get(j).get(0)).add(blockSims.get(i).get(j).get(2));
					}
					else {
						thisBlock.add(blockSims.get(i).get(j).get(0));
						thisBlock.add(blockSims.get(i).get(j).get(1));
						transitive.put(blockSims.get(i).get(j).get(0), new ArrayList<Double> ());
						transitive.get(blockSims.get(i).get(j).get(0)).add(blockSims.get(i).get(j).get(1));
						transitive.get(blockSims.get(i).get(j).get(0)).add(blockSims.get(i).get(j).get(2));
					}
				}
			}	
			System.out.println("\nADDING ZEROS: " + transitive + "\n");
			for (Double d: transitive.keySet()) {
				if (transitive.get(d).size() > 2 && thisBlock.contains((d)))
					transitive.get(d).add(new Double(0));
			}
			System.out.println("\nADDING ZEROS: " + transitive + "\n");
			intraClusterDistance(thisBlock);
			System.out.println("TRANSITIVE" + transitive + "\n");
			outer:
			for (int j = 0; j < transitive.keySet().toArray().length; j++) {
				Double d1 = (Double) transitive.keySet().toArray()[j];
				for (int k = 0; k < transitive.get(d1).size() - 1; k += 2) {
					System.out.println("GOT HERE");
					Double d2 = transitive.get(d1).get(k);
					if (transitive.keySet().contains(d2)) {
						if (transitive.get(d1).containsAll(transitive.get(d2))) {		
							System.out.println("FIRST");
							transitive.remove(d2);
							continue;
						}
						if (transitive.get(d1).get(transitive.get(d1).size() - 1) < transitive.get(d2).get(transitive.get(d2).size() - 1)) {
							int toRemove = transitive.get(d1).indexOf(d2);
							transitive.get(d1).remove(toRemove + 1);
							transitive.get(d1).remove(toRemove);
							if (transitive.get(d1).size() == 0) {
								System.out.println("SECOND");
								transitive.remove(d1);
								continue outer;
							}
							intraClusterDistance(thisBlock);
						}
						else {
							System.out.println("THIRD");
							transitive.remove(d2);
						}
					}
					else {
						for (int m = j + 1; m  < transitive.keySet().toArray().length; m++) {
							System.out.println("SECOND: " + m + ": " + transitive.get(transitive.keySet().toArray()[m]));
							if (transitive.get(transitive.keySet().toArray()[m]).contains(d2)) {
								System.out.println("CONTAINS " + d2);
								if (transitive.get(d1).get(transitive.get(d1).size() - 1) < 
										transitive.get(transitive.keySet().toArray()[m]).get(transitive.get(transitive.keySet().toArray()[m]).size() - 1)) {
									transitive.get(d1).remove(transitive.keySet().toArray()[m]);
									if (transitive.get(d1).size() == 0) {
										transitive.remove(d1);
										System.out.println("FOURTH");
										continue outer;
									}
									intraClusterDistance(thisBlock);
								}
								else {
									System.out.println("IN REMOVE");
									int toRemove = transitive.get(transitive.keySet().toArray()[m]).indexOf(d2);
									System.out.println("D2: " + d2 );
									System.out.println("Transitive: " + transitive.get(transitive.keySet().toArray()[m]));
									System.out.println("to remove: " + toRemove);
									transitive.get(transitive.keySet().toArray()[m]).remove(toRemove + 1);
									transitive.get(transitive.keySet().toArray()[m]).remove(toRemove);
									if (transitive.get(transitive.keySet().toArray()[m]).size() == 0) {
										System.out.println("FIFTH");
										transitive.remove(transitive.keySet().toArray()[m]);
									}
								}
							}
						}
					}
					System.out.println("JUST BEFORE TURNING: " + transitive);
				}
			}
		}
		System.out.println("++++++++++++");
		System.out.println(transitive);
		System.out.println("++++++++++++");
	}
	
	private void writeOutput() throws KettleStepException, KettlePluginException {
		
		for (int i = 0; i < data.buffer.size(); i++) {
			Object[] newRow = new Object[data.buffer.get(i).length + 2];
			for (int j = 0; j < data.buffer.get(i).length; j++) 
				newRow[j] = data.buffer.get(i)[j];
			RowMeta rowMeta = new RowMeta();
			rowMeta.addValueMeta(ValueMetaFactory.createValueMeta( meta.getGroupColumnName(), ValueMetaInterface.TYPE_INTEGER ));
			rowMeta.addValueMeta(ValueMetaFactory.createValueMeta( meta.getSimColumnName(), ValueMetaInterface.TYPE_NUMBER ));		
			
			long group = i + 1;
			Double outputSimilarity = null;
			Double index = new Double(i + 1);		

			if (transitive.containsKey(index)) {
				group = index.longValue();
			}
			else {
				for (Double j: transitive.keySet()) {
					if (transitive.get(j).contains(index)) {
						group = j.longValue();
						outputSimilarity = transitive.get(j).get(transitive.get(j).indexOf(index) + 1);
						
						DecimalFormatSymbols symbols = new DecimalFormatSymbols(Locale.getDefault());
						symbols.setDecimalSeparator('.');
						DecimalFormat df = new DecimalFormat("#.#", symbols);
						df.setRoundingMode(RoundingMode.DOWN);
						outputSimilarity = Double.parseDouble(df.format(outputSimilarity));
						
						break;
					}					
				}
			}
			
			RowMetaAndData newRowMD = new RowMetaAndData(rowMeta, new Object[] { group, 
					outputSimilarity});
			newRow = RowDataUtil.addRowData( newRow, getInputRowMeta().size(), newRowMD.getData() );
						
			putRow( data.getOutputRowMeta(), newRow);
		}
	}
	
	private void intraClusterDistance(Set<Double> thisBlock) {
		Set<Double> keySet = transitive.keySet();
		for(Double d: keySet) {
			if (!thisBlock.contains(d)) 
				continue;
			if (transitive.get(d).size() > 2) {
				double avg = 0;
				for (int j = 1; j < transitive.get(d).size(); j += 2) {
					avg += transitive.get(d).get(j);
				}
				double measure = (1 / Math.floor(transitive.get(d).size()/ 2)) * avg;
				transitive.get(d).set(transitive.get(d).size() - 1, measure);
			}
		}
	}
}