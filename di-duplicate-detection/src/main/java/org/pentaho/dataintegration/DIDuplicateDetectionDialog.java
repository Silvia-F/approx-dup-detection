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
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.events.*;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Props;
import org.pentaho.di.core.plugins.PluginInterface;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.ui.core.ConstUI;
import org.pentaho.di.ui.core.FormDataBuilder;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

public class DIDuplicateDetectionDialog extends BaseStepDialog implements StepDialogInterface {

	private static Class<?> PKG = DIDuplicateDetectionMeta.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

	private static final int MARGIN_SIZE = 15;
	private static final int LABEL_SPACING = 5;
	private static final int ELEMENT_SPACING = 10;

	private static final int LARGE_FIELD = 350;
	private static final int MEDIUM_FIELD = 250;
	private static final int SMALL_FIELD = 75;

	private DIDuplicateDetectionMeta meta;

	private Text wThreshold;
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

	public DIDuplicateDetectionDialog( Shell parent, Object in, TransMeta tr, String sname ) {
		super( parent, (BaseStepMeta) in, tr, sname );
		meta = (DIDuplicateDetectionMeta) in;
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
		shell.setText( BaseMessages.getString( PKG, "DIDuplicateDetectionDialog.Shell.Title" ) );

		//Step name label and text field
		wlStepname = new Label( shell, SWT.RIGHT );
		wlStepname.setText( BaseMessages.getString( PKG, "DIDuplicateDetectionDialog.Stepname.Label" ) );
		props.setLook( wlStepname );

		fdlStepname = new FormDataBuilder()
				.left( 0, 0 )
				.right( props.getMiddlePct(), -Const.MARGIN )
				.top( 0, Const.MARGIN )
				.result();
		wlStepname.setLayoutData( fdlStepname );

		wStepname = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
		wStepname.setText( stepname );
		props.setLook( wStepname );
		wStepname.addModifyListener( lsMod );

		fdStepname = new FormDataBuilder()
				.left( props.getMiddlePct(), 0 )
				.right( 100, -Const.MARGIN )
				.top( 0, Const.MARGIN )
				.result();
		wStepname.setLayoutData( fdStepname );

		// Step parameters
		Label wlThreshold = new Label( shell, SWT.RIGHT );
		wlThreshold.setText( BaseMessages.getString( PKG, "DIDuplicateDetectionDialog.Threshold.Label" ) );
		props.setLook( wlThreshold );

		FormData fdlThreshold = new FormDataBuilder()
				.left( 0, 0 )
				.right( props.getMiddlePct(), -Const.MARGIN )
				.top( wStepname, 4 * Const.MARGIN )
				.result();
		wlThreshold.setLayoutData( fdlThreshold );

		wThreshold = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
		props.setLook( wThreshold );
		wThreshold.addModifyListener( lsMod );

		FormData fdThreshold = new FormDataBuilder()
				.left( props.getMiddlePct(), 0 )
				.right( 100, -Const.MARGIN )
				.top( wStepname, 2 * Const.MARGIN )
				.result();
		wThreshold.setLayoutData( fdThreshold );  

		Label wlGroupColumnName = new Label(shell, SWT.RIGHT);
		wlGroupColumnName.setText( BaseMessages.getString( PKG, "DIDuplicateDetectionDialog.GroupColumnName.Label" ) );
		props.setLook(wlGroupColumnName);

		FormData fdlGroupColumnName = new FormDataBuilder()
				.left( 0, 0 )
				.right( props.getMiddlePct(), -Const.MARGIN )
				.top( wThreshold, 4 * Const.MARGIN )
				.result();
		wlGroupColumnName.setLayoutData( fdlGroupColumnName );

		wGroupColumnName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
		props.setLook(wGroupColumnName);
		wGroupColumnName.addModifyListener(lsMod);

		FormData fdGroupColumnName = new FormDataBuilder()
				.left( props.getMiddlePct(), 0 )
				.right( 100, -Const.MARGIN )
				.top( wThreshold, 4 * Const.MARGIN )
				.result();
		wGroupColumnName.setLayoutData( fdGroupColumnName );
		
		Label wlSimColumnName = new Label(shell, SWT.RIGHT);
		wlSimColumnName.setText( BaseMessages.getString( PKG, "DIDuplicateDetectionDialog.SimColumnName.Label" ) );
		props.setLook(wlSimColumnName);

		FormData fdlSimColumnName = new FormDataBuilder()
				.left( 0, 0 )
				.right( props.getMiddlePct(), -Const.MARGIN )
				.top( wGroupColumnName, 4 * Const.MARGIN )
				.result();
		wlSimColumnName.setLayoutData( fdlSimColumnName );

		wSimColumnName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
		props.setLook(wSimColumnName);
		wSimColumnName.addModifyListener(lsMod);

		FormData fdSimColumnName = new FormDataBuilder()
				.left( props.getMiddlePct(), 0 )
				.right( 100, -Const.MARGIN )
				.top( wGroupColumnName, 4 * Const.MARGIN )
				.result();
		wSimColumnName.setLayoutData( fdSimColumnName );
		
		Label wlRemoveSingletons = new Label( shell, SWT.RIGHT );
		wlRemoveSingletons.setText( BaseMessages.getString( PKG, "DIDuplicateDetectionDialog.RemoveSingletons.Label" ) );
		props.setLook( wlRemoveSingletons );

		FormData fdlRemoveSingletons = new FormDataBuilder()
				.left( 0, 0 )
				.right( props.getMiddlePct(), -Const.MARGIN )
				.top( wSimColumnName, 4 * Const.MARGIN )
				.result();
		wlRemoveSingletons.setLayoutData( fdlRemoveSingletons );

		wRemoveSingletons = new Button(shell, SWT.CHECK);
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
	
	public void getFields() {
		wThreshold.setText(String.valueOf(meta.getMatchThreshold()));
		wGroupColumnName.setText(meta.getGroupColumnName());
		wSimColumnName.setText(meta.getSimColumnName());
		wRemoveSingletons.setSelection(meta.getRemoveSingletons());
		
	}

	private void ok() {
		meta.setMatchThrehsold(Double.parseDouble(wThreshold.getText()));
		meta.setGroupColumnName(wGroupColumnName.getText());
		meta.setSimColumnName(wSimColumnName.getText());
		meta.setRemoveSingletons(wRemoveSingletons.getSelection());
		stepname = wStepname.getText();
		dispose();
	}
	
	private void cancel() {
		dispose();
	}
}