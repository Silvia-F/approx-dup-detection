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
 * This class holds meta data for fields.
 */
@Step( id = "ApproxDupDetection", image = "ApproxDupDetection.svg", name = "Approximate Duplicate Detection",
		description = "Approximate Duplicate Detection Step", categoryDescription = "Lookup" )

public class ApproxDupDetectionMeta extends BaseStepMeta implements StepMetaInterface {
  
	private static Class<?> PKG = ApproxDupDetection.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$
	private String matchMethod;
	private double matchThreshold;
	
	public ApproxDupDetectionMeta() {
		super(); // allocate BaseStepMeta
	}
	
	public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {
		readData( stepnode );
	}
	
	public String getXML() {		
		StringBuilder retval = new StringBuilder(300);
		retval.append(XMLHandler.addTagValue("matchThreshold", matchThreshold)).append(Const.CR);
		return retval.toString();
	}		
	
	public Object clone() {
		Object retval = super.clone();
		return retval;
	}
	  
	private void readData( Node stepnode ) {
		String tempThreshold = XMLHandler.getTagValue(stepnode, "matchThreshold");
		try {
			if(tempThreshold != null) {
				matchThreshold = Double.parseDouble(tempThreshold);
			}
		} catch(Exception ex) {
			matchThreshold = 0;
		}
	}
	
	public void setDefault() {
		matchThreshold = 0.6;
		matchMethod = "Domain-Independent";
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
			ValueMetaInterface v = ValueMetaFactory.createValueMeta( "Group",  ValueMetaInterface.TYPE_INTEGER );
			rowMeta.addValueMeta( v );
		} catch (KettlePluginException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
	
	public double getMatchThreshold() {
		return matchThreshold;
	}
	
	public void setMatchThreshold(double threshold) {
		matchThreshold = threshold;
	}
}
