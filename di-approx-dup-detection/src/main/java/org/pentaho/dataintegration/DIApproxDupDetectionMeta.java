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

import java.util.List;


/**
 * Skeleton for PDI Step plugin.
 */
@Step( id = "DIApproxDupDetection", image = "DIApproxDupDetection.svg", name = "DI Approximate Duplicate Detection",
description = "Domain Independent Approximate Duplicate Detection", categoryDescription = "Lookup" )
public class DIApproxDupDetectionMeta extends BaseStepMeta implements StepMetaInterface {

	private static Class<?> PKG = DIApproxDupDetection.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$
	
	private String groupColumnName; // The name for the output column of approximate duplicate groups
	private String simColumnName; // The name for the output column corresponding to the similarity values
	private double matchThreshold; // The matching threshold value
	private boolean removeSingletons; // If true, remove singleton groups from the output

	public DIApproxDupDetectionMeta() {
		super(); // allocate BaseStepMeta
	}

	public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {
		readData( stepnode );
	}

	public Object clone() {
		Object retval = super.clone();
		return retval;
	}

	private void readData( Node stepnode ) {
		groupColumnName = XMLHandler.getTagValue(stepnode, "groupColumnName");
		simColumnName = XMLHandler.getTagValue(stepnode, "simColumnName");
		try {
			matchThreshold = Double.parseDouble(XMLHandler.getTagValue(stepnode, "matchThreshold"));
		} catch (Exception e) {
			matchThreshold = 0.6;
		}
		try {
			removeSingletons = Boolean.parseBoolean(XMLHandler.getTagValue(stepnode, "removeSingletons"));
		} catch (Exception e) {
			removeSingletons = false;
		}
	}
	
	public String getXML() {		
		StringBuilder retval = new StringBuilder(300);
		retval.append(XMLHandler.addTagValue("groupColumnName", groupColumnName)).append(Const.CR);
		retval.append(XMLHandler.addTagValue("simColumnName", simColumnName)).append(Const.CR);
		retval.append(XMLHandler.addTagValue("matchThreshold", matchThreshold)).append(Const.CR);
		retval.append(XMLHandler.addTagValue("removeSingletons", String.valueOf(removeSingletons))).append(Const.CR);
		return retval.toString();
	}		

	public void setDefault() {
		groupColumnName = "Group";
		simColumnName = "Similarity";
		matchThreshold = 0.6;
		removeSingletons = false;
	}

	public void readRep( Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases ) throws KettleException {
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
			cr = new CheckResult( CheckResultInterface.TYPE_RESULT_WARNING, BaseMessages.getString( PKG, "DIApproxDupDetectionMeta.CheckResult.NotReceivingFields" ), stepMeta ); 
			remarks.add( cr );
		}
		else {
			cr = new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString( PKG, "DIApproxDupDetectionMeta.CheckResult.StepRecevingData", prev.size() + "" ), stepMeta );  
			remarks.add( cr );
		}

		// See if we have input streams leading to this step!
		if ( input.length > 0 ) {
			cr = new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString( PKG, "DIApproxDupDetectionMeta.CheckResult.StepRecevingData2" ), stepMeta ); 
			remarks.add( cr );
		}
		else {
			cr = new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString( PKG, "DIApproxDupDetectionMeta.CheckResult.NoInputReceivedFromOtherSteps" ), stepMeta ); 
			remarks.add( cr );
		}
	}

	public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta tr, Trans trans ) {
		return new DIApproxDupDetection( stepMeta, stepDataInterface, cnr, tr, trans );
	}

	public StepDataInterface getStepData() {
		return new DIApproxDupDetectionData();
	}

	public String getDialogClassName() {
		return "org.pentaho.dataintegration.DIApproxDupDetectionDialog";
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
	
	public void setMatchThrehsold(double matchThreshold) {
		this.matchThreshold = matchThreshold;
	}
	
	public double getMatchThreshold() {
		return matchThreshold;
	}
	
	public void setRemoveSingletons(boolean removeSingletons) {
		this.removeSingletons = removeSingletons;
	}
	
	public boolean getRemoveSingletons() {
		return removeSingletons;
	}
}
