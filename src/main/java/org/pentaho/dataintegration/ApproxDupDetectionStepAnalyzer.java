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

import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.metaverse.api.IMetaverseNode;
import org.pentaho.metaverse.api.MetaverseAnalyzerException;
import org.pentaho.metaverse.api.StepField;
import org.pentaho.metaverse.api.analyzer.kettle.step.StepAnalyzer;

import java.util.HashSet;
import java.util.Set;

public class ApproxDupDetectionStepAnalyzer extends StepAnalyzer<ApproxDupDetectionMeta> {
  @Override
  protected Set<StepField> getUsedFields( ApproxDupDetectionMeta meta ) {
    // no incoming fields are used by the Dummy step
    return null;
  }

  @Override
  protected void customAnalyze( ApproxDupDetectionMeta meta, IMetaverseNode rootNode ) throws MetaverseAnalyzerException {
    // add any custom properties or relationships here
    rootNode.setProperty( "do_nothing", true );
  }

  @Override
  public Set<Class<? extends BaseStepMeta>> getSupportedSteps() {
    Set<Class<? extends BaseStepMeta>> supportedSteps = new HashSet<>();
    supportedSteps.add( ApproxDupDetectionMeta.class );
    return supportedSteps;
  }
}
