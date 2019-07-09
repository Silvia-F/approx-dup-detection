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
import org.apache.commons.lang.StringUtils;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleValueException;
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
			first = false;
		}
		data.buffer.add(r);
		data.incrementIndex();
		for (int i = 0; i < getInputRowMeta().getFieldNames().length; i++) {
			if (getInputRowMeta().getFieldNames()[i].equals(meta.getBlockingAttribute())) {
				ArrayList<String> fields = new ArrayList<String> ();
				for (int j = 0; j < meta.getMatchFields().size(); j++) {
					for (int k = 0; k < getInputRowMeta().getFieldNames().length; k++) {
						if (meta.getMatchFields().get(j) != null && meta.getMatchFields().get(j).equals(getInputRowMeta().getFieldNames()[k])) {
							fields.add(getInputRowMeta().getString(r, k));
							break;
						}
					}
				}
				String fieldValue = getInputRowMeta().getString(r, i);
				if (data.getBlocks().containsKey(fieldValue)) {
					data.getBlocks().get(fieldValue).add(data.getIndex());
					data.getBlocks().get(fieldValue).add(fields);
				}
				else {
					data.getBlocks().put(fieldValue, new ArrayList<Object> ());
					data.getBlocks().get(fieldValue).add(data.getIndex());
					data.getBlocks().get(fieldValue).add(fields);
				}
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
		double threshold = meta.getMatchingThreshold();
		Set<String> keys = data.getBlocks().keySet();
		for (String s: keys) {
			ArrayList<ArrayList<Double>> blockSims = new ArrayList<ArrayList<Double>>();
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
										((double)Math.max(a.get(k).length(), b.get(k).length())));
								break;
							case(2):								
								similarity += meta.getConvertedMeasures()[k][1] * (1 - Math.abs(new NeedlemanWunsch().score(a.get(k), b.get(k)))) /
										((double)Math.max(a.get(k).length(), b.get(k).length()));
								break;
							case(3):
								similarity += meta.getConvertedMeasures()[k][1] * new Jaro().score(a.get(k), b.get(k));
								break;
							case(4):
								similarity += meta.getConvertedMeasures()[k][1] * new JaroWinkler().score(a.get(k), b.get(k));
								break;
							case(5):
								similarity += meta.getConvertedMeasures()[k][1] * LetterPairSimilarity.getSimiliarity(a.get(k), b.get(k));
								//pair letter
								break;
							case(6):
								//metaphone [get dophonetic() from fuzzy match]
								break;
							case(7):
								//double matephone [get dophonetic() from fuzzy match]
								break;
							case(8):
								//soundex [get dophonetic() from fuzzy match]
								break;
							case(9):
								// refined soundex [get dophonetic() from fuzzy match]
								break;
						}
					}
					ArrayList<Double> temp = new ArrayList<Double>();
					temp.add(((Integer)data.getBlocks().get(s).get(i - 1)).doubleValue());
					temp.add(((Integer)data.getBlocks().get(s).get(j - 1)).doubleValue());
					temp.add(similarity);
					blockSims.add(temp);
				}
			}	
			Double first = ((Integer)data.getBlocks().get(s).get(0)).doubleValue();
			data.getRulesSim().put(first, new Double[] {first, new Double(-1)});
			for (int i = 0; i < data.getBlocks().get(s).size(); i += 2) {
				Double recordIndex = ((Integer)data.getBlocks().get(s).get(i)).doubleValue();
				double maxSim = 0;
				double maxIndex = 0;
				for (int j = 0; j < blockSims.size(); j++) {
					if (blockSims.get(j).get(2) < threshold)
						continue;
					if (blockSims.get(j).contains(recordIndex) && blockSims.get(j).get(2) > maxSim) {	
						maxIndex = blockSims.get(j).get(1) == recordIndex ? blockSims.get(j).get(0): blockSims.get(j).get(1);
						maxSim = blockSims.get(j).get(2);
					}
				}
				data.getRulesSim().put(recordIndex, new Double[] { maxIndex, (Double)maxSim });
			}				
		}
	}
	
	private void writeOutput() throws KettleStepException, KettlePluginException {
		for (int i = 0; i < data.buffer.size(); i++) {
			Object[] newRow = new Object[data.buffer.get(i).length + 2];
			for (int j = 0; j < data.buffer.get(i).length; j++) 
				newRow[j] = data.buffer.get(i)[j];
			RowMeta rowMeta = new RowMeta();
			rowMeta.addValueMeta(ValueMetaFactory.createValueMeta( meta.getGroupColumnName(), ValueMetaInterface.TYPE_INTEGER ));
			rowMeta.addValueMeta(ValueMetaFactory.createValueMeta( meta.getSimColumnName(), ValueMetaInterface.TYPE_NUMBER ));		
			
			Double outputSimilarity = null;
			Double d = (Double)((double)(i + 1));
			if (data.getRulesSim().get(d)[0] == 0) {
				System.out.println("WHY AM I HERE?");
				data.getRulesSim().get(d)[0] = d;	
			}
			if (! d.equals(data.getRulesSim().get(d)[0])) {					
				DecimalFormatSymbols symbols = new DecimalFormatSymbols(Locale.getDefault());
				symbols.setDecimalSeparator('.');
				DecimalFormat df = new DecimalFormat("#.#", symbols);
			 
				df.setRoundingMode(RoundingMode.DOWN);
				outputSimilarity = Double.parseDouble(df.format(data.getRulesSim().get(d)[1]));
			}
			System.out.println("OUTPUT: ");
			System.out.println(d +" | " + data.getRulesSim().get(d)[0] + " | " + data.getRulesSim().get(d)[0].longValue());
			RowMetaAndData newRowMD = new RowMetaAndData(rowMeta, new Object[] { data.getRulesSim().get(d)[0].longValue(), 
					outputSimilarity});
			newRow = RowDataUtil.addRowData( newRow, getInputRowMeta().size(), newRowMD.getData() );
						
			putRow( data.getOutputRowMeta(), newRow);
		}
	}
}