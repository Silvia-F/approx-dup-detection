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

import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.List;


/**
 * This class holds meta data for fields.
 */
@Step( id = "ApproxDupDetection", image = "ApproxDupDetection.svg", name = "Approximate Duplicate Detection",
		description = "Approximate Duplicate Detection Step", categoryDescription = "Lookup", documentationUrl = "https://web.ist.utl.pt/ist181041/approx-dup-detection.html" )

public class ApproxDupDetectionMeta extends BaseStepMeta implements StepMetaInterface {
  
	private static Class<?> PKG = ApproxDupDetection.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$
	
	private ArrayList<String> blockingAttributes; // Attributes used to perform blocking in the rule-based approach
	private double matchingThreshold; // Matching threshold fot the total similarity
	private ArrayList<String> matchFields; // Keeps which fields to match
	private double[][] measures; // Keeps the weight of each field along with the similarity measure to use for each field
	private double[][] convertedMeasures; //Keeps similar information to the measures matrix, but the weights are normalized
	private String groupColumnName; // Name of the output column with approximate duplicate groups	
	private String simColumnName; // Name of the output column with field similarity
	private boolean removeSingletons; // If true, singleton approximate duplicate groups will be removed from the output
	private boolean removeDuplicates; // If true, only the first record of each group will be added to the output
	
	
	public ApproxDupDetectionMeta() {
		super(); // allocate BaseStepMeta
	}
	
	public void allocate(int nrFields) {
		blockingAttributes = new ArrayList<String> ();
		this.matchFields = new ArrayList<String> ();
		this.measures = new double[nrFields][2];
		this.convertedMeasures = new double[nrFields][2];
	}
	
	public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {
		readData( stepnode );
	}
	
	public String getXML() {		
		StringBuilder retval = new StringBuilder(300);
		for (int i = 0; i < blockingAttributes.size(); i++) {
			retval.append("<blockingAttribute>").append(Const.CR);
			retval.append("    " + XMLHandler.addTagValue("attributeName", blockingAttributes.get(i)));
			retval.append("</blockingAttribute>").append(Const.CR);
		}
		retval.append(XMLHandler.addTagValue("matchingThreshold", matchingThreshold)).append(Const.CR);
		for (int i = 0; i < matchFields.size(); i++) {
			retval.append("<matchField>").append(Const.CR);
			retval.append("    " + XMLHandler.addTagValue("fieldName", matchFields.get(i)));
			retval.append("</matchField>").append(Const.CR);
		}
		for (int i = 0; i < measures.length; i++) {
			retval.append("<measure>").append(Const.CR);
			retval.append("    " + XMLHandler.addTagValue("name", measures[i][0]));
			retval.append("    " + XMLHandler.addTagValue("weight", measures[i][1]));
			retval.append("</measure>").append(Const.CR);
		}
		retval.append(XMLHandler.addTagValue("groupColumnName", groupColumnName)).append(Const.CR);
		retval.append(XMLHandler.addTagValue("simColumnName", simColumnName)).append(Const.CR);
		retval.append(XMLHandler.addTagValue("removeSingletons", String.valueOf(removeSingletons))).append(Const.CR);
		retval.append(XMLHandler.addTagValue("removeDuplicates", String.valueOf(removeDuplicates))).append(Const.CR);
		
		return retval.toString();
	}		
	
	public Object clone() {
		Object retval = super.clone();
		return retval;
	}
	  
	private void readData( Node stepnode ) {
		int nrFields = XMLHandler.countNodes(stepnode, "matchField");
		allocate(nrFields);
		
		int nrAttributes = XMLHandler.countNodes(stepnode, "blockingAttribute");
		for (int i = 0; i < nrAttributes; i++) {
			System.out.println(stepnode);
			Node node = XMLHandler.getSubNodeByNr(stepnode, "blockingAttribute", i);
			blockingAttributes.add(XMLHandler.getTagValue(node, "attributeName"));
		}
		String tempThreshold = XMLHandler.getTagValue(stepnode, "matchingThreshold");
		try {
			if(tempThreshold != null) {
				matchingThreshold = Double.parseDouble(tempThreshold);
			}
		} catch(Exception ex) {
			matchingThreshold = 0;
		}
		
		for (int i = 0; i < nrFields; i++) {
			Node node1 = XMLHandler.getSubNodeByNr(stepnode, "matchField", i);
			matchFields.add(XMLHandler.getTagValue(node1, "fieldName"));

			Node node2 = XMLHandler.getSubNodeByNr(stepnode, "measure", i);
			try {
				measures[i] = new double[] {
					Double.parseDouble(XMLHandler.getTagValue(node2, "name")),
					Double.parseDouble(XMLHandler.getTagValue(node2, "weight"))
				};
			} catch(Exception ex) {
				measures[i] = new double[] { 0, 0};
			}			
		}
		groupColumnName = XMLHandler.getTagValue(stepnode, "groupColumnName");
		simColumnName = XMLHandler.getTagValue(stepnode, "simColumnName");
		removeSingletons = Boolean.parseBoolean(XMLHandler.getTagValue(stepnode, "removeSingletons"));
		removeDuplicates = Boolean.parseBoolean(XMLHandler.getTagValue(stepnode, "removeDuplicates"));
	}
	
	public void setDefault() {
		matchingThreshold = 0.5;
		groupColumnName = "Group";
		simColumnName = "Similarity";
		removeSingletons = false;
		removeDuplicates = false;
		allocate(0);		
	}
	
	public void readRep( Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases ) 
			throws KettleException {
	}
	  
	public void saveRep( Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step )
			throws KettleException {
	}
	  
	public void getFields( RowMetaInterface rowMeta, String origin, RowMetaInterface[] info, StepMeta nextStep, 
			VariableSpace space, Repository repository, IMetaStore metaStore ) throws KettleStepException {
		
		try {
			ValueMetaInterface v = ValueMetaFactory.createValueMeta( getGroupColumnName(),  ValueMetaInterface.TYPE_INTEGER );
			rowMeta.addValueMeta( v );
		} catch (KettlePluginException e) {
			System.out.println("Problem while adding new row meta!");
		}
		try {
			ValueMetaInterface v = ValueMetaFactory.createValueMeta( getSimColumnName(),  ValueMetaInterface.TYPE_NUMBER );
			rowMeta.addValueMeta( v );
		} catch (KettlePluginException e) {
			System.out.println("Problem while adding new row meta!");
		}
	}
	  
	public void check( List<CheckResultInterface> remarks, TransMeta transMeta, 
			StepMeta stepMeta, RowMetaInterface prev, String input[], String output[],
			RowMetaInterface info, VariableSpace space, Repository repository, 
			IMetaStore metaStore ) {
		CheckResult cr;
		if ( prev == null || prev.size() == 0 ) {
			cr = new CheckResult( CheckResultInterface.TYPE_RESULT_WARNING, BaseMessages.getString( PKG, "ApproxDupDetectionMeta.CheckResult.NotReceivingFields" ), stepMeta ); 
			remarks.add( cr );
		}
		else {
			cr = new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString( PKG, "ApproxDupDetectionMeta.CheckResult.StepRecevingData", prev.size() + "" ), stepMeta );  
			remarks.add( cr );
		}
	    
		// See if we have input streams leading to this step!
		if ( input.length > 0 ) {
			cr = new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString( PKG, "ApproxDupDetectionMeta.CheckResult.StepRecevingData2" ), stepMeta ); 
			remarks.add( cr );
		}
		else {
			cr = new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString( PKG, "ApproxDupDetectionMeta.CheckResult.NoInputReceivedFromOtherSteps" ), stepMeta ); 
			remarks.add( cr );
		}
	}
	  
	public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta tr, Trans trans ) {
		return new ApproxDupDetection( stepMeta, stepDataInterface, cnr, tr, trans );
	}
	  
	public StepDataInterface getStepData() {
		return new ApproxDupDetectionData();
	}
	
	public String getDialogClassName() {
		return "org.pentaho.dataintegration.ApproxDupDetectionDialog";
	}
	
	public ArrayList<String> getBlockingAttributes() {
		return blockingAttributes;
	}
	
	public double getMatchingThreshold() {
		return matchingThreshold;
	}
	
	public void setMatchingThreshold(double threshold) {
		matchingThreshold = threshold;
	}
	
	public ArrayList<String> getMatchFields() {
		return matchFields;
	}
	
	public void setMatchFields(ArrayList<String> matchFields) {
		this.matchFields = matchFields;
	}
	
	public double[][] getMeasures() {
		return measures;
	}
	
	public void setMeasures(double[][] measures) {
		this.measures = measures;
	}

	public double[][] getConvertedMeasures() {
		return convertedMeasures;
	}
	
	public void setGroupColumnName(String groupColumnName) {
		this.groupColumnName = groupColumnName;
	}
	
	public String getGroupColumnName() {
		return groupColumnName;
	}
	
	public void setSimColumnName(String simColumnName) {
		this.simColumnName = simColumnName;
	}
	
	public String getSimColumnName() {
		return simColumnName;
	}
	
	public void setRemoveSingletons(boolean removeSingletons) {
		this.removeSingletons = removeSingletons;
	}
	
	public boolean getRemoveSingletons() {
		return removeSingletons;
	}
	
	public void setRemoveDuplicates(boolean removeDuplicates) {
		this.removeDuplicates = removeDuplicates;
	}
	
	public boolean getRemoveDuplicates() {
		return removeDuplicates;
	}
}
