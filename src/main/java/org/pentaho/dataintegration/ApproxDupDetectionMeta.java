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
		description = "Approximate Duplicate Detection Step", categoryDescription = "Lookup" )

public class ApproxDupDetectionMeta extends BaseStepMeta implements StepMetaInterface {
  
	private static Class<?> PKG = ApproxDupDetection.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$
	
	private String matchMethod; // States which match method to compute
	private String columnName; // Keeps the name of the output column for the approximate duplicate groups
	private ArrayList<String> matchFields; // Keeps which fields to match in the rule approach
	private double[][] measures; // Keeps the weight of each field along with the similarity measure to use for each field
	private double[][] convertedMeasures; //Keeps similar information to the measures matrix, but the weights are normalized.
	private double matchThresholdDI; // Keeps the matching threshold value for the domain-independent approach
	private double matchThresholdRule; // Keeps the matching threshold value for the rule-based approach
	private boolean cartesianProduct; // If true, the cartesian product of the data is done for the rule-based approach
	private String blockingAttribute; // Attribute used to perform blocking in the rule-based approach
	private double blockingThreshold; // Similarity threhold to build blocks for the rule-based approach
	private boolean removeSingletons; // If true, singleton approximate duplicate groups will be removed from the output
	
	
	public ApproxDupDetectionMeta() {
		super(); // allocate BaseStepMeta
	}
	
	public void allocate(int nrFields) {
		this.matchFields = new ArrayList<String>();
		this.measures = new double[nrFields][2];
		this.convertedMeasures = new double[nrFields][2];
	}
	
	public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {
		readData( stepnode );
	}
	
	public String getXML() {		
		StringBuilder retval = new StringBuilder(300);
		retval.append(XMLHandler.addTagValue("columnName", columnName)).append(Const.CR);
		retval.append(XMLHandler.addTagValue("removeSingletons", removeSingletons)).append(Const.CR);
		retval.append(XMLHandler.addTagValue("matchMethod", matchMethod)).append(Const.CR);
		retval.append(XMLHandler.addTagValue("matchThresholdDI", matchThresholdDI)).append(Const.CR);
		for (int i = 0; i < matchFields.size(); i++) {
			retval.append("<matchField>").append(Const.CR);
			retval.append("    " + XMLHandler.addTagValue("fieldName", matchFields.get(i)));
			retval.append("</matchField>").append(Const.CR);
		}
		retval.append(XMLHandler.addTagValue("matchThresholdRule", matchThresholdRule)).append(Const.CR);
		for (int i = 0; i < measures.length; i++) {
			retval.append("<measure>").append(Const.CR);
			retval.append("    " + XMLHandler.addTagValue("name", measures[i][0]));
			retval.append("    " + XMLHandler.addTagValue("weight", measures[i][1]));
			retval.append("</measure>").append(Const.CR);
		}
		retval.append(XMLHandler.addTagValue("cartesianProduct", cartesianProduct)).append(Const.CR);
		if (! cartesianProduct) {
			retval.append(XMLHandler.addTagValue("blockingAttribute", blockingAttribute)).append(Const.CR);
			retval.append(XMLHandler.addTagValue("blockingThreshold", blockingThreshold)).append(Const.CR);
		}			
		return retval.toString();
	}		
	
	public Object clone() {
		Object retval = super.clone();
		return retval;
	}
	  
	private void readData( Node stepnode ) {
		matchMethod = XMLHandler.getTagValue(stepnode, "matchMethod");
		columnName = XMLHandler.getTagValue(stepnode, "columnName");
		removeSingletons = Boolean.parseBoolean(XMLHandler.getTagValue(stepnode, "removeSingletons"));
		String tempThresholdDI = XMLHandler.getTagValue(stepnode, "matchThresholdDI");
		try {
			if(tempThresholdDI != null) {
				matchThresholdDI = Double.parseDouble(tempThresholdDI);
			}
		} catch(Exception ex) {
			matchThresholdDI = 0;
		}
		String tempThresholdRule = XMLHandler.getTagValue(stepnode, "matchThresholdRule");
		try {
			if(tempThresholdRule != null) {
				matchThresholdRule = Double.parseDouble(tempThresholdRule);
			}
		} catch(Exception ex) {
			matchThresholdRule = 0;
		}
		int nrFields = XMLHandler.countNodes(stepnode, "matchField");
		allocate(nrFields);
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
		cartesianProduct = Boolean.parseBoolean(XMLHandler.getTagValue(stepnode, "cartesianProduct"));
		if (! cartesianProduct) {
			blockingAttribute = XMLHandler.getTagValue(stepnode, "blockingAttribute");
			try {
				blockingThreshold = Double.parseDouble(XMLHandler.getTagAttribute(stepnode, "blockingThreshold"));
			} catch(Exception ex) {
				blockingThreshold = 0.3;
			}
				
		}
	}
	
	public void setDefault() {
		matchMethod = "Domain-Independent";
		columnName = "Group";
		matchThresholdDI = 0.6;
		matchThresholdRule = 0.5;
		cartesianProduct = false;
		blockingThreshold = 0.3;
		removeSingletons = false;
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
			ValueMetaInterface v = ValueMetaFactory.createValueMeta( getColumnName(),  ValueMetaInterface.TYPE_INTEGER );
			rowMeta.addValueMeta( v );
		} catch (KettlePluginException e) {
			System.out.println("Problem while adding new row meta!");
		}
		try {
			ValueMetaInterface v = ValueMetaFactory.createValueMeta( "Similarity",  ValueMetaInterface.TYPE_NUMBER );
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
	
	public String getMatchMethod() {
		return matchMethod;
	}
	
	public void setMatchMethod(String method) {
		this.matchMethod = method;
	}
	
	public String getColumnName() {
		return columnName;
	}
	
	public void setColumnName(String name) {
		this.columnName = name;
	}
	
	public double getMatchThresholdDI() {
		return matchThresholdDI;
	}
	
	public void setMatchThresholdDI(double threshold) {
		matchThresholdDI = threshold;
	}
	
	public double getMatchThresholdRule() {
		return matchThresholdRule;
	}
	
	public void setMatchThresholdRule(double threshold) {
		matchThresholdRule = threshold;
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
	
	public void setCartesianProduct(boolean cartesianProduct) {
		this.cartesianProduct = cartesianProduct;
	}
	
	public boolean getCartesianProduct() {
		return cartesianProduct;
	}
	
	public void setBlockingAttribute(String attribute) {
		this.blockingAttribute = attribute;
	}
	
	public String getBlockingAttribute() {
		return blockingAttribute;
	}
	
	public void setBlockingThreshold(double threshold) {
		this.blockingThreshold = threshold;
	}
	
	public double getBlockingThreshold() {
		return blockingThreshold;
	}
	
	public void setRemoveSingletons(boolean removeSingletons) {
		this.removeSingletons = removeSingletons;
	}
	
	public boolean getRemoveSingletons() {
		return removeSingletons;
	}
}
