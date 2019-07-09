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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.events.*;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.ui.core.FormDataBuilder;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.ComboValuesSelectionListener;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

public class ApproxDupDetectionDialog extends BaseStepDialog implements StepDialogInterface {

	private static Class<?> PKG = ApproxDupDetectionMeta.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

	private ApproxDupDetectionMeta meta;
	
	private Combo wBlockingAttribute;
	private Text wMatchingThreshold;	
	private ColumnInfo[] colinf;
	private TableView wFields;
	private Text wGroupColumnName;
	private Text wSimColumnName;
	private Button wRemoveSingletons;	
	private Button wCancel;
	private Button wOK;
	private ModifyListener lsMod;
	private Listener lsCancel;
	private Listener lsOK;
	private SelectionAdapter lsDef;
	private boolean changed;
	
	private HashMap<String, String[]> fieldMeasures;
	private ArrayList<String> fields;
	private ArrayList<String> added;

	public ApproxDupDetectionDialog( Shell parent, Object in, TransMeta tr, String sname ) {
		super( parent, (BaseStepMeta) in, tr, sname );
		meta = (ApproxDupDetectionMeta) in;
		fieldMeasures = new HashMap<String, String[]> ();
		fields = new ArrayList<String> ();
		added = new ArrayList<String> ();
	}

	public String open() {
		// Set up window
		Shell parent = getParent();
		Display display = parent.getDisplay();

		shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX );
		props.setLook( shell);
		setShellImage( shell, meta );

		lsMod = new ModifyListener() {
			public void modifyText( ModifyEvent e ) {
				meta.setChanged();
			}
		};
		changed = meta.hasChanged();

		
		// Margins
		FormLayout formLayout = new FormLayout();
		formLayout.marginLeft = Const.FORM_MARGIN;
		formLayout.marginHeight = Const.FORM_MARGIN;
		shell.setLayout( formLayout );
		shell.setText( BaseMessages.getString( PKG, "ApproxDupDetectionDialog.Shell.Title" ) );
		
		
		// Composites to keep content
		ScrolledComposite scrolledComposite = new ScrolledComposite(shell, SWT.V_SCROLL);
		Composite contentComposite = new Composite(scrolledComposite, SWT.NONE);
		FormLayout contentLayout = new FormLayout();
		contentLayout.marginRight = Const.MARGIN;
		contentComposite.setLayout( contentLayout );
		FormData compositeLayoutData = new FormDataBuilder().fullSize()
				.result();
		contentComposite.setLayoutData( compositeLayoutData );
		props.setLook( contentComposite );

		
		//Step name label and text field
		wlStepname = new Label( contentComposite, SWT.RIGHT );
		wlStepname.setText( BaseMessages.getString( PKG, "ApproxDupDetectionDialog.Stepname.Label" ) );
		props.setLook( wlStepname );

		fdlStepname = new FormDataBuilder()
				.left( 0, 0 )
				.right( props.getMiddlePct(), -Const.MARGIN )
				.top( 0, Const.MARGIN )
				.result();
		wlStepname.setLayoutData( fdlStepname );
		
		wStepname = new Text( contentComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
		wStepname.setText( stepname );
		props.setLook( wStepname );
		wStepname.addModifyListener( lsMod );

		fdStepname = new FormDataBuilder()
				.left( props.getMiddlePct(), 0 )
				.right( 100, -Const.MARGIN )
				.top( 0, Const.MARGIN )
				.result();
		wStepname.setLayoutData( fdStepname );

		
		// Blocking Content
		Group group1 = new Group( contentComposite, SWT.SHADOW_ETCHED_IN );
		group1.setText( BaseMessages.getString( PKG, "ApproxDupDetectionDialog.Group1"));
		FormLayout groupLayout = new FormLayout();
		groupLayout.marginWidth = Const.MARGIN;
		groupLayout.marginHeight = Const.MARGIN;
		group1.setLayout( groupLayout );
		FormData fdGroup = new FormDataBuilder().fullWidth()
				.left()
				.top(wStepname, 15)
				.result();
		group1.setLayoutData(fdGroup);	
		props.setLook(group1);
	
		Label wlBlockingAttribute = new Label(group1, SWT.RIGHT);
		wlBlockingAttribute.setText( BaseMessages.getString( PKG, "ApproxDupDetectionDialog.BlockingAttribute.Label" ) );
		props.setLook(wlBlockingAttribute);
		
		FormData fdlBlockingAttribute = new FormDataBuilder()
				.left( 0, 0 )
				.right( props.getMiddlePct(), -Const.MARGIN )
				.top( wStepname, 4 * Const.MARGIN )
				.result();
		wlBlockingAttribute.setLayoutData( fdlBlockingAttribute );
		
		wBlockingAttribute = new Combo( group1, SWT.READ_ONLY | SWT.LEFT | SWT.BORDER );
		props.setLook(wBlockingAttribute);
		wBlockingAttribute.setItems(fields.toArray(new String[0]));
		wBlockingAttribute.addModifyListener(lsMod);
		
		FormData fdBlockingAttribute = new FormDataBuilder()
				.left( props.getMiddlePct(), 0 )
				.right( 100, -Const.MARGIN )
				.top( wStepname, 4 * Const.MARGIN )
				.result();
		wBlockingAttribute.setLayoutData( fdBlockingAttribute );
		
		
		// Matching Rules Content
		Group group2 = new Group( contentComposite, SWT.SHADOW_ETCHED_IN );
		group2.setText( BaseMessages.getString( PKG, "ApproxDupDetectionDialog.Group2"));
		FormLayout group2Layout = new FormLayout();
		group2Layout.marginWidth = Const.MARGIN;
		group2Layout.marginHeight = Const.MARGIN;
		group2.setLayout( group2Layout );
		FormData fdGroup2 = new FormDataBuilder().fullWidth()
				.left()
				.top(group1, 15)
				.result();
		group2.setLayoutData(fdGroup2);	
		props.setLook(group2);		
		
		Label wlMatchingThreshold = new Label( group2, SWT.RIGHT );
		wlMatchingThreshold.setText( BaseMessages.getString( PKG, "ApproxDupDetectionDialog.Threshold.Label" ) );
		props.setLook( wlMatchingThreshold );

		FormData fdlMatchingThreshold = new FormDataBuilder()
				.left( 0, 0 )
				.right( props.getMiddlePct(), -Const.MARGIN )
				.top( wBlockingAttribute, 2 * Const.MARGIN )
				.result();
		wlMatchingThreshold.setLayoutData( fdlMatchingThreshold );
		
		wMatchingThreshold = new Text( group2, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
		props.setLook( wMatchingThreshold );
		wMatchingThreshold.addModifyListener( lsMod );

		FormData fdMatchingThreshold = new FormDataBuilder()
				.left( props.getMiddlePct(), 0 )
				.right( 100, -Const.MARGIN )
				.top( wBlockingAttribute, 2 * Const.MARGIN )
				.result();
		wMatchingThreshold.setLayoutData( fdMatchingThreshold );		
		
		Label wlFields = new Label( group2, SWT.NONE );
		wlFields.setText( BaseMessages.getString( PKG, "ApproxDupDetectionDialog.Fields.Label" ) );
		props.setLook( wlFields );
		FormData fdlFields = new FormDataBuilder()
				.left( 0, 0 )
				.top( wMatchingThreshold, Const.MARGIN )
				.result();
		wlFields.setLayoutData( fdlFields );

		int fieldsRows = 0;
		try {
			fieldsRows = transMeta.getPrevStepFields( transMeta.findStep( stepname ) ).size();
		} catch (KettleStepException e1) {
			e1.printStackTrace();
		}

		colinf = new ColumnInfo[] {
				new ColumnInfo(
						BaseMessages.getString( PKG, "ApproxDupDetectionDialog.FieldName.Column" ),
						ColumnInfo.COLUMN_TYPE_CCOMBO, false, true, 100 ),
				new ColumnInfo(
						BaseMessages.getString( PKG, "ApproxDupDetectionDialog.Measure.Column" ),
						ColumnInfo.COLUMN_TYPE_CCOMBO, false, false, 100 ),
				new ColumnInfo(
						BaseMessages.getString( PKG, "ApproxDupDetectionDialog.Weight.Column" ),
						ColumnInfo.COLUMN_TYPE_TEXT, false, false, 100 )
		};

		wFields = new TableView(transMeta, group2, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, 
				colinf, fieldsRows, lsMod, props );

		FormData fdFields = new FormDataBuilder()
				.left( 0, 0 )
				.right( 100, 0 )
				.top(  wlFields, Const.MARGIN )
				.result();
		wFields.setLayoutData( fdFields );

		colinf[0].setComboValuesSelectionListener( new ComboValuesSelectionListener() {
			@Override
			public String[] getComboValues(TableItem arg0, int arg1, int arg2) {
				fieldMeasures.remove(arg0.getText(1));
				String[] fieldNames = fields.toArray( new String[fields.size()] );
				Const.sortStrings( fieldNames );
				return fieldNames;
			}			
		});
		
		colinf[1].setComboValuesSelectionListener( new ComboValuesSelectionListener() {
			@Override
			public String[] getComboValues(TableItem arg0, int arg1, int arg2) {
				return new String[] {"Levenshtein", "Damerau Levenshtein", "Needleman Wunsch", "Jaro", 
						"Jaro Winkler", "Pair letters Similarity", "Metaphone", "Double Metaphone", 
						"SoundEx", "Refined SoundEx"};
			}
		});

		// Search the fields in the background
		final Runnable runnable = new Runnable() {
			public void run() {
				StepMeta stepMeta = transMeta.findStep( stepname );
				if ( stepMeta != null ) {
					try {
						RowMetaInterface row = transMeta.getPrevStepFields( stepMeta );
						// Remember these fields...
						for ( int i = 0; i < row.size(); i++ ) {
							ValueMetaInterface field = row.getValueMeta( i );
							fields.add( field.getName()); 
						}
					} catch ( KettleException e ) {
						logError( BaseMessages.getString( PKG, "ApproxDupDetectionDialog.Log.UnableToFindInput" ) );
					}
				}
			}
		};
		new Thread( runnable ).start();
		
		wFields.addModifyListener( new ModifyListener() {
			public void modifyText( ModifyEvent arg0 ) {
				if (arg0.getSource().toString().equals("TableView {}")) { //Is this a valid way to detect fields being deleted?
					ArrayList<String> items =  new ArrayList<String>(Arrays.asList(wFields.getItems(0)));
					for (int i = 0; i < added.size(); i++) {
						if (!items.contains(added.get(i)))
							added.remove(i);
					}							
				}
				meta.setChanged();
			}
		} );
		
		
		// Output Content
		Group group3 = new Group( contentComposite, SWT.SHADOW_ETCHED_IN );
		group3.setText( BaseMessages.getString( PKG, "ApproxDupDetectionDialog.Group3"));
		FormLayout group3Layout = new FormLayout();
		group3Layout.marginWidth = Const.MARGIN;
		group3Layout.marginHeight = Const.MARGIN;
		group3.setLayout( group3Layout );
		FormData fdGroup3 = new FormDataBuilder().fullWidth()
				.left()
				.top(group2, 15)
				.result();
		group3.setLayoutData(fdGroup3);	
		props.setLook(group3);	
			
		Label wlGroupColumnName = new Label(group3, SWT.RIGHT);
		wlGroupColumnName.setText( BaseMessages.getString( PKG, "ApproxDupDetectionDialog.GroupColumnName.Label" ) );
		props.setLook(wlGroupColumnName);
		
		FormData fdlGroupColumnName = new FormDataBuilder()
				.left( 0, 0 )
				.right( props.getMiddlePct(), -Const.MARGIN )
				.top( group2, 4 * Const.MARGIN )
				.result();
		wlGroupColumnName.setLayoutData( fdlGroupColumnName );
		
		wGroupColumnName = new Text( group3, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
		props.setLook(wGroupColumnName);
		wGroupColumnName.addModifyListener(lsMod);
		
		FormData fdGroupColumnName = new FormDataBuilder()
				.left( props.getMiddlePct(), 0 )
				.right( 100, -Const.MARGIN )
				.top( group2, 4 * Const.MARGIN )
				.result();
		wGroupColumnName.setLayoutData( fdGroupColumnName );
		
		Label wlSimColumnName = new Label(group3, SWT.RIGHT);
		wlSimColumnName.setText( BaseMessages.getString( PKG, "ApproxDupDetectionDialog.SimColumnName.Label" ) );
		props.setLook(wlSimColumnName);
		
		FormData fdlSimColumnName = new FormDataBuilder()
				.left( 0, 0 )
				.right( props.getMiddlePct(), -Const.MARGIN )
				.top( wGroupColumnName, 4 * Const.MARGIN )
				.result();
		wlSimColumnName.setLayoutData( fdlSimColumnName );
		
		wSimColumnName = new Text( group3, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
		props.setLook(wSimColumnName);
		wSimColumnName.addModifyListener(lsMod);
		
		FormData fdSimColumnName = new FormDataBuilder()
				.left( props.getMiddlePct(), 0 )
				.right( 100, -Const.MARGIN )
				.top( wGroupColumnName, 4 * Const.MARGIN )
				.result();
		wSimColumnName.setLayoutData( fdSimColumnName );
		
		wRemoveSingletons = new Button(group3, SWT.CHECK);
		wRemoveSingletons.setText( BaseMessages.getString( PKG, "ApproxDupDetectionDialog.RemoveSingletons" ) );
		props.setLook(wRemoveSingletons);
		
		FormData fdRemoveSingletons = new FormDataBuilder()
				.left( props.getMiddlePct(), 0 )
				.right( 100, -Const.MARGIN )
				.top( wSimColumnName, 4 * Const.MARGIN )
				.result();
		wRemoveSingletons.setLayoutData( fdRemoveSingletons );
		
		
		//Cancel and OK buttons for the bottom of the window.
		wCancel = new Button( shell, SWT.PUSH );
		wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
		FormData fdCancel = new FormDataBuilder()
				.right(60, -Const.MARGIN)
				.bottom()
				.result();
				wCancel.setLayoutData( fdCancel );
	
		wOK = new Button( shell, SWT.PUSH );
		wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
		FormData fdOk = new FormDataBuilder()
				.right( wCancel, -Const.MARGIN )
				.bottom()
				.result();
				wOK.setLayoutData( fdOk );
		
	
		//Listeners		
		lsCancel = new Listener() {
			public void handleEvent( Event e ) {
				cancel();
			}
		};
		lsOK = new Listener() {
			public void handleEvent( Event e ) {
				ok();
			}
		};
	
		wOK.addListener( SWT.Selection, lsOK );
		wCancel.addListener( SWT.Selection, lsCancel );
	
		lsDef = new SelectionAdapter() {
			public void widgetDefaultSelected( SelectionEvent e ) {
				ok();
			}
		};
		wStepname.addSelectionListener( lsDef );
	
		shell.addShellListener( new ShellAdapter() {
			public void shellClosed( ShellEvent e ) {
				cancel();
			}
		} );
		
		
		// Add everything to the scrolled composite
		scrolledComposite.setContent(contentComposite);
		scrolledComposite.setExpandVertical(true);
		scrolledComposite.setExpandHorizontal(true);
		scrolledComposite.setMinSize( contentComposite.computeSize( SWT.DEFAULT, SWT.DEFAULT ) );

		scrolledComposite.setLayout(new FormLayout());
		FormData fdScrolledComposite = new FormDataBuilder().fullWidth()
				.top()
				.bottom(wOK, -20)
				.result();
		scrolledComposite.setLayoutData(fdScrolledComposite);
		props.setLook(scrolledComposite);
	
		//get parameters from meta
		getFields();
		
		//Show shell
		setSize();
		meta.setChanged( changed );
		shell.open();
		while ( !shell.isDisposed() ) {
			if ( !display.readAndDispatch() ) {
				display.sleep();
			}
		}
		return stepname;
	}
	
	private void getFields() {
		if (meta.getBlockingAttribute() != null)
			wBlockingAttribute.setText(meta.getBlockingAttribute());
		
		wMatchingThreshold.setText(String.valueOf(meta.getMatchingThreshold()));
		
		ArrayList<String> measureNames = new ArrayList<String>();
		measureNames.add("Levenshtein");
		measureNames.add("Damerau Levenshtein");
		measureNames.add("Needleman Wunsch");
		measureNames.add("Jaro");
		measureNames.add("Jaro Winkler");
		measureNames.add("Pair letters Similarity");
		measureNames.add("Metaphone");
		measureNames.add("Double Metaphone");
		measureNames.add("SoundEx");
		measureNames.add("Refined SoundEx");
		int rowCount = 0;
		for (int i = 0; i < meta.getMatchFields().size(); i++) {
			if (meta.getMatchFields().get(i) != null) {
				wFields.table.getItem(rowCount).setText(new String[] {String.valueOf(rowCount + 1), meta.getMatchFields().get(i), 
						measureNames.get((int)meta.getMeasures()[i][0]), String.valueOf(meta.getMeasures()[i][1])});
			}			
			rowCount++;
		}
		
		wGroupColumnName.setText(meta.getGroupColumnName());
		wSimColumnName.setText(meta.getSimColumnName());
		wRemoveSingletons.setSelection(meta.getRemoveSingletons());
	}

	private void cancel() {
		dispose();
	}
	
	private void ok() {
		stepname = wStepname.getText();
		
		if (wBlockingAttribute.getText() != null)
			meta.setBlockingAttribute(wBlockingAttribute.getText());
		
		meta.setMatchingThreshold(Double.parseDouble(wMatchingThreshold.getText()));
		
		int nrFields = wFields.nrNonEmpty(); 
		meta.allocate(nrFields);
		ArrayList<String> tempFields = new ArrayList<String>();
		double[][] tempMeasures = new double[nrFields][];
		ArrayList<String> measureNames = new ArrayList<String>();
		measureNames.add("Levenshtein");
		measureNames.add("Damerau Levenshtein");
		measureNames.add("Needleman Wunsch");
		measureNames.add("Jaro");
		measureNames.add("Jaro Winkler");
		measureNames.add("Pair letters Similarity");
		measureNames.add("Metaphone");
		measureNames.add("Double Metaphone");
		measureNames.add("SoundEx");
		measureNames.add("Refined SoundEx");		
		for (int i = 0; i < nrFields; i++) {
			if (wFields.table.getItem(i).getText(1).length() > 0) {
				tempFields.add(wFields.table.getItem(i).getText(1));	
				double measure = measureNames.indexOf(wFields.table.getItem(i).getText(2));			
				double weight = Double.parseDouble(wFields.table.getItem(i).getText(3));	
				tempMeasures[i] = new double[] {measure, weight};				
			}
		}
		meta.setMatchFields(tempFields);
		meta.setMeasures(tempMeasures);
		
		if (wGroupColumnName.getText().length() > 0)
			meta.setGroupColumnName(wGroupColumnName.getText());
		if (wSimColumnName.getText().length() > 0)
			meta.setSimColumnName(wSimColumnName.getText());
		meta.setRemoveSingletons(wRemoveSingletons.getSelection());
		dispose();
	}
}