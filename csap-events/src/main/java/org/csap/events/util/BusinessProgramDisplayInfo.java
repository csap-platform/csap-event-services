package org.csap.events.util ;

import java.util.List ;

public class BusinessProgramDisplayInfo {

	private String packageName ;
	private String displayName ;
	private boolean hidden ;
	private List healthEnabledLife ;
	private List lifecycle ;

	public String getPackageName ( ) {

		return packageName ;

	}

	public void setPackageName ( String packageName ) {

		this.packageName = packageName ;

	}

	public String getDisplayName ( ) {

		return displayName ;

	}

	public void setDisplayName ( String displayName ) {

		this.displayName = displayName ;

	}

	public boolean isHidden ( ) {

		return hidden ;

	}

	public void setHidden ( boolean hidden ) {

		this.hidden = hidden ;

	}

	public List getHealthEnabledLife ( ) {

		return healthEnabledLife ;

	}

	public void setHealthEnabledLife ( List healthEnabledLife ) {

		this.healthEnabledLife = healthEnabledLife ;

	}

	public List getLifecycle ( ) {

		return lifecycle ;

	}

	public void setLifecycle ( List lifecycle ) {

		this.lifecycle = lifecycle ;

	}

}
