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
import java.util.LinkedHashMap;
import java.util.LinkedList;
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
	private ArrayList<RecordGroup> recordGroups;
  
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
			recordGroups = new ArrayList<RecordGroup> ();
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
			
			// Prepare for cartesian product
			data.getBlocks().put("", new ArrayList<Object>() );
			
			first = false;
		}
		
		data.buffer.add(r);
		data.incrementIndex();
		
		// Cartesian Product
		if (meta.getBlockingAttributes().size() == 0) {
			ArrayList<String> fields = new ArrayList<String> ();
			for (int i = 0; i < meta.getMatchFields().size(); i++) {
				for (int j = 0; j < getInputRowMeta().getFieldNames().length; j++) {
					if (meta.getMatchFields().get(i) != null && meta.getMatchFields().get(i).equals(getInputRowMeta().getFieldNames()[j])) {
						fields.add(getInputRowMeta().getString(r, j));
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
						if (getInputRowMeta().getString(r, j) != null)							
							blockingValue = blockingValue.concat(getInputRowMeta().getString(r, j));
						break;
					}
				}
			}
			ArrayList<String> fields = new ArrayList<String> ();
			for (int i = 0; i < meta.getMatchFields().size(); i++) {
				for (int j = 0; j < getInputRowMeta().getFieldNames().length; j++) {
					if (meta.getMatchFields().get(i) != null && meta.getMatchFields().get(i).equals(getInputRowMeta().getFieldNames()[j])) {
						fields.add(getInputRowMeta().getString(r, j));
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
		Set<String> keys = data.getBlocks().keySet(); 
		// Iterate through each block
		for (String s: keys) {
			ArrayList<ArrayList<Double>> blockSim = new ArrayList<ArrayList<Double>>();
			for (int i = 1; i < data.getBlocks().get(s).size(); i +=  2) {
				ArrayList<String> a = (ArrayList<String>)data.getBlocks().get(s).get(i); //First record to compare
				for (int j = i + 2; j < data.getBlocks().get(s).size(); j += 2) {
					ArrayList<String> b = (ArrayList<String>)data.getBlocks().get(s).get(j); //Second record to compare
					double similarity = 0;
					// Iterate through the fields that were chosen to perform the matching
					for (int k = 0; k < meta.getConvertedMeasures().length; k++) { 
						String first = a.get(k) != null ? a.get(k) : "";
						String second = b.get(k) != null ? b.get(k) : "";
						if (first.length() + second.length() == 0) {
							continue;
						}
						switch ((int)meta.getConvertedMeasures()[k][0]) {
							case(0):								
								similarity += meta.getConvertedMeasures()[k][1] * (1 - StringUtils.getLevenshteinDistance(first, second) / 
										(double)Math.max(first.length(), first.length()));
								break;
							case(1):
								similarity += meta.getConvertedMeasures()[k][1] * (1 - Utils.getDamerauLevenshteinDistance(first, second) /
										(double)Math.max(first.length(), second.length()));
								break;
							case(2):								
								similarity += meta.getConvertedMeasures()[k][1] * (1 - Math.abs(new NeedlemanWunsch().score(first, second)) /
										(double)Math.max(first.length(), second.length()));
								break;
							case(3):
								similarity += meta.getConvertedMeasures()[k][1] * new Jaro().score(first, second);
								break;
							case(4):
								similarity += meta.getConvertedMeasures()[k][1] * new JaroWinkler().score(first, second);
								break;
							case(5):
								similarity += meta.getConvertedMeasures()[k][1] * LetterPairSimilarity.getSimiliarity(first, second);
								break;
							//Starting here we only have phonetic measures. These only output 0 or 1 as similarity values.
							case(6):
								String metaphone1 = (new Metaphone()).metaphone(first);
								String metaphone2 = (new Metaphone()).metaphone(second);
								if (metaphone1.equals(metaphone2))
									similarity += meta.getConvertedMeasures()[k][1];
								break;
							case(7):
								String doubleMetaphone1 = (new DoubleMetaphone()).doubleMetaphone(first);
								String doubleMetaphone2 = (new DoubleMetaphone()).doubleMetaphone(second);
								if (doubleMetaphone1.equals(doubleMetaphone2))
									similarity += meta.getConvertedMeasures()[k][1];
								break;
							case(8):
								String soundex1 = (new Soundex()).encode(first);
								String soundex2 = (new Soundex()).encode(second);
								if (soundex1.equals(soundex2))
									similarity += meta.getConvertedMeasures()[k][1];
								break;
							case(9):
								String refSoundex1 = (new RefinedSoundex()).encode(first);
								String refSoundex2 = (new RefinedSoundex()).encode(second);
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
			if (blockSim.size() > 0)
				createGroups(blockSim);
		}	
	}


	
	private void createGroups(ArrayList<ArrayList<Double>> block) {
		ArrayList<RecordGroup> groups = new ArrayList<RecordGroup> ();
		Double first = new Double(-1); // First element of the group, to be used as an id.
		int index = -1; // index of the current group in the list
		for (ArrayList<Double> lst: block) {
			if (first.equals(lst.get(0))) {
				if (lst.get(2) >= meta.getMatchingThreshold())
					groups.get(index).addElement(lst);
			}
			else {							
				if (lst.get(2) >= meta.getMatchingThreshold()) {
					RecordGroup group = new RecordGroup(lst.get(0));	
					first = lst.get(0);
					group.addElement(lst);
					groups.add(group);
					index++;
				}
			}
		}
		for (int i = 0; i < groups.size(); i++) {
			RecordGroup group1 = groups.get(i);
			for (int j = i+ 1; j < groups.size(); j++) {
				RecordGroup group2 = groups.get(j);
				if (group1.getElements().containsAll(group2.getElements())) {
					groups.remove(group2); 
					j--;
				}
				else {
					// Iterate through the group's elements to see if it belongs to more than one group
					for (int = 0) { 
						
					}
				}
				
			}
		}
		
	}

	private void writeOutput() throws KettleStepException, KettlePluginException {		
		/*HashMap<Double, ArrayList<Double>> mapping = new HashMap<Double, ArrayList<Double>> ();
		while (recordGroups.size() > 0) {
			for (Double elem: recordGroups.get(0).getElements()) {
				ArrayList<Double> temp = new ArrayList<Double> ();
				temp.add(recordGroups.get(0).getId());
				temp.add(recordGroups.get(0).getOutputSims().get(elem));
				mapping.put(elem, temp);
			}
			recordGroups.remove(0);
		}
		for (int i = 0; i < data.buffer.size(); i++) {
			Object[] newRow = new Object[data.buffer.get(i).length + 2];
			for (int j = 0; j < data.buffer.get(i).length; j++) 
				newRow[j] = data.buffer.get(i)[j];
			RowMeta rowMeta = new RowMeta();
			rowMeta.addValueMeta(ValueMetaFactory.createValueMeta( meta.getGroupColumnName(), ValueMetaInterface.TYPE_INTEGER ));
			rowMeta.addValueMeta(ValueMetaFactory.createValueMeta( meta.getSimColumnName(), ValueMetaInterface.TYPE_NUMBER ));		
			
			Double index = new Double(i + 1);
			long group = i + 1;
			Double outputSimilarity = null;

			if (mapping.containsKey(index)) {
				group = mapping.get(index).get(0).longValue();
				
				outputSimilarity = mapping.get(index).get(1);
				DecimalFormatSymbols symbols = new DecimalFormatSymbols(Locale.getDefault());
				symbols.setDecimalSeparator('.');
				DecimalFormat df = new DecimalFormat("#.#", symbols);
				df.setRoundingMode(RoundingMode.DOWN);
				outputSimilarity = Double.parseDouble(df.format(outputSimilarity));
			}
			
			else if (meta.getRemoveSingletons())
				continue;
			
			RowMetaAndData newRowMD = new RowMetaAndData(rowMeta, new Object[] { group, 
					outputSimilarity});
			newRow = RowDataUtil.addRowData( newRow, getInputRowMeta().size(), newRowMD.getData() );
						
			putRow( data.getOutputRowMeta(), newRow);
		}*/
	}
}