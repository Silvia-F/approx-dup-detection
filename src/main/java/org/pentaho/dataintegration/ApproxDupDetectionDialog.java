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

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.*;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Props;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.ui.core.FormDataBuilder;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

public class ApproxDupDetectionDialog extends BaseStepDialog implements StepDialogInterface {

	private static Class<?> PKG = ApproxDupDetectionMeta.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

	private ApproxDupDetectionMeta meta;

	private static final int TAB_WIDTH = 450;
	
	private Text wThreshold;
	private Button wRadioButton1;
	private Button wCancel;
	private Button wOK;
	private ModifyListener lsMod;
	private Listener lsCancel;
	private Listener lsOK;
	private SelectionAdapter lsDef;
	private boolean changed;

	public ApproxDupDetectionDialog( Shell parent, Object in, TransMeta tr, String sname ) {
		super( parent, (BaseStepMeta) in, tr, sname );
		meta = (ApproxDupDetectionMeta) in;
	}

	public String open() {
		//Set up window
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

		//Margins
		FormLayout formLayout = new FormLayout();
		formLayout.marginLeft = Const.FORM_MARGIN;
		formLayout.marginHeight = Const.FORM_MARGIN;
		shell.setLayout( formLayout );
		shell.setText( BaseMessages.getString( PKG, "ApproxDupDetectionDialog.Shell.Title" ) );

		//Step name label and text field
		wlStepname = new Label( shell, SWT.RIGHT );
		wlStepname.setText( BaseMessages.getString( PKG, "ApproxDupDetectionDialog.Stepname.Label" ) );
		props.setLook( wlStepname );

		fdlStepname = new FormData();
		fdlStepname.left = new FormAttachment( 0, 0 );
		fdlStepname.right = new FormAttachment( props.getMiddlePct(), -Const.MARGIN );
		fdlStepname.top = new FormAttachment( 0, Const.MARGIN );
		wlStepname.setLayoutData( fdlStepname );
		
		wStepname = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
		wStepname.setText( stepname );
		props.setLook( wStepname );
		wStepname.addModifyListener( lsMod );

		fdStepname = new FormData();
		fdStepname.left = new FormAttachment( props.getMiddlePct(), 0 );
		fdStepname.top = new FormAttachment( 0, Const.MARGIN );
		fdStepname.right = new FormAttachment( 100, -Const.MARGIN );
		wStepname.setLayoutData( fdStepname );

		//Tabs
		CTabFolder wTabFolder = new CTabFolder( shell, SWT.BORDER );
		props.setLook( wTabFolder, Props.WIDGET_STYLE_TAB );
		
		FormData fdTabFolder = new FormData();
		fdTabFolder.top = new FormAttachment(wStepname, Const.MARGIN * 5);
		fdTabFolder.width = TAB_WIDTH;
		wTabFolder.setLayoutData( fdTabFolder );

		CTabItem wTab1 = new CTabItem( wTabFolder, SWT.NONE );
		wTab1.setText( BaseMessages.getString( PKG, "ApproxDupDetectionDialog.Tab1" ) );
		Composite wTab1Contents = new Composite( wTabFolder, SWT.SHADOW_NONE );
		props.setLook( wTab1Contents );
		FormLayout tab1Layout = new FormLayout();
		tab1Layout.marginWidth = Const.MARGIN;
		tab1Layout.marginHeight = Const.MARGIN;
		wTab1Contents.setLayout( tab1Layout );
		FormData fdTab1 = new FormDataBuilder().fullSize()
				.result();
		wTab1Contents.setLayoutData( fdTab1 );
		wTab1.setControl( wTab1Contents );

		CTabItem wTab2 = new CTabItem( wTabFolder, SWT.NONE );
		wTab2.setText( BaseMessages.getString( PKG, "ApproxDupDetectionDialog.Tab2" ) );
		Composite wTab2Contents = new Composite( wTabFolder, SWT.NONE );
		props.setLook( wTab2Contents );
		FormLayout tab2Layout = new FormLayout();
		tab2Layout.marginWidth = Const.MARGIN;
		tab2Layout.marginHeight = Const.MARGIN;
		wTab2Contents.setLayout( tab2Layout );
		FormData fdTab2 = new FormDataBuilder().fullSize()
				.result();
		wTab2Contents.setLayoutData( fdTab2 );
		wTab2.setControl( wTab2Contents );

		wTabFolder.setSelection( 0 );

		//Content for the first tab
		wRadioButton1 = new Button(wTab1Contents, SWT.RADIO);
		wRadioButton1.setText( BaseMessages.getString( PKG, "ApproxDupDetectionDialog.RadioButton1"));
		wRadioButton1.setBackground( display.getSystemColor( SWT.COLOR_TRANSPARENT ) );
		FormData fdRadioButton1 = new FormDataBuilder().left()
		.top()
		.result();
		wRadioButton1.setLayoutData(fdRadioButton1);
		
		
		//Contents for the second tab
		Label wlThreshold = new Label( wTab2Contents, SWT.RIGHT );
		wlThreshold.setText( BaseMessages.getString( PKG, "ApproxDupDetectionDialog.Threshold.Label" ) );
		props.setLook( wlThreshold );

		FormData fdlThreshold = new FormData();
		fdlThreshold.top = new FormAttachment(wTabFolder, 2 * Const.MARGIN);
		fdlThreshold.left = new FormAttachment( 0, 0 );
		fdlThreshold.right = new FormAttachment( props.getMiddlePct(), -Const.MARGIN );
		wlThreshold.setLayoutData( fdlThreshold );
		
		wThreshold = new Text( wTab2Contents, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
		wThreshold.setText( "0.6" );
		props.setLook( wThreshold );
		wThreshold.addModifyListener( lsMod );

		FormData fdThreshold = new FormData();
		fdThreshold.left = new FormAttachment( props.getMiddlePct(), 0 );
		fdThreshold.right = new FormAttachment( 100, -Const.MARGIN );
		wThreshold.setLayoutData( fdThreshold );
		
		//Cancel and OK buttons for the bottom of the window.
		wCancel = new Button( shell, SWT.PUSH );
		wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
	
		wOK = new Button( shell, SWT.PUSH );
		wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
	
		setButtonPositions( new Button[] { wOK, wCancel }, Const.MARGIN, null );
	
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
		if (meta.getMatchThreshold() != 0)
			wThreshold.setText(String.valueOf(meta.getMatchThreshold()));
	}

	private void cancel() {
		dispose();
	}
	
	private void ok() {
		stepname = wStepname.getText();
		meta.setMatchThreshold(Double.parseDouble(wThreshold.getText()));
		dispose();
	}
}