$(document).ready(function() {
	var admin = new Admin();
	admin.appInit();

});
function Admin() {
	this.appInit = function() {
		console.log("Init in admin");
		loadBusinessProgramDisplayInfo();
	};
	
	function loadBusinessProgramDisplayInfo(){
		$.getJSON("api/report/displayInformation", {			
		}) 
		.success(function(loadJson) {			
			loadBusinessProgramDisplayInfoSuccess(loadJson);
		});
	}
	
	function loadBusinessProgramDisplayInfoSuccess(dataJson){						
		$('#businessProgramBody').empty();
		var i  = 0;
		for(var packageName in dataJson){			
			var businessProgramTrContent = '<td style="font-size: 1.0em" > ' + packageName +'</td>'+
			'<td style="font-size: 1.0em" > '  +  getDisplayNameTextInput(packageName,dataJson[packageName].displayName,i) +'</td>'+
			'<td style="font-size: 1.0em" > '  +   getHideCheckBox(packageName,dataJson[packageName].hidden,i) +'</td>'
			+'<td style="font-size: 1.0em" > '  +   constructHealthByLife(packageName,dataJson[packageName],i) +'</td>'
			;
			var businessProgramTr = $('<tr />', {
				'class' : "",
				html : businessProgramTrContent
			});
			$('#businessProgramBody').append(businessProgramTr);
			i++;	
		}
		registerOnChange();
		registerOnChangeHide();
		registerOnChangeHealthy();
	}
	
	function getDisplayNameTextInput(bussProgram,displayName, counter){
		var textId = 'displayname'+counter;
		var dataTextId = 'data-'+textId;
		var textInput = "<input class='bussProgramRename' style='width:15em;' "+ dataTextId +"='"+bussProgram +"'id='"+textId + "' value='"+ displayName + "'/>";
		return textInput;
	}
	
	function constructHealthByLife(bussProgram,dataForBussProg,counter){
		var healthCheck =""
		var lifecycles = dataForBussProg.lifecycle;
		lifecycles.sort();
		for(var i=0; i < lifecycles.length; i++){
			var life = lifecycles[i];
			var isHealth = isHealthSelected(life,dataForBussProg.healthEnabledLife);
			healthCheck = healthCheck + constructHealthCheckBox(bussProgram,isHealth,i,life);
		}
		return healthCheck;
	}
	function isHealthSelected(life,currentSelection){
		if(currentSelection){
			for(var j = 0 ; j < currentSelection.length; j++){
				if(life == currentSelection[j]){
					return true;
				}
			}
		}
		return false;
	}
	
	function constructHealthCheckBox(bussProgram,health,counter,life){
		var checkInput = "";
		var bussProgName = bussProgram.replace(/\s+/g, '');
		var checkBoxId = bussProgName+"_" + life + "_" + counter;
		checkBoxId = checkBoxId.toLowerCase();
		var dataText = ' data-'+checkBoxId +"='"+bussProgram +"' ";
		if(health == true){
			checkInput = life + "<input checked='checked' class='bussProgramHealthy' type='checkbox'"+ dataText  +"id='"+checkBoxId +"' />";
		} else {
			checkInput = life + "<input class='bussProgramHealthy' type='checkbox'"+ dataText  +"id='"+checkBoxId +"' />";
		}
		return checkInput;
	}

	
	function getHideCheckBox(bussProgram,hide,counter){
		var checkInput = "";
		var checkBoxId = 'hide'+counter;
		var dataText = ' data-'+checkBoxId +"='"+bussProgram +"' ";
		if(hide == true){
			checkInput = "<input checked='checked' class='bussProgramHide' type='checkbox'"+ dataText +"id='"+checkBoxId +"' />";
		} else {
			checkInput = "<input class='bussProgramHide' type='checkbox'"+ dataText +"id='"+checkBoxId +"' />";
		}
		return checkInput;
	}
	
	function getUpdateButton(counter){
		var pushButton = "<a class='pushButton'>Update</a>";
		return pushButton;
	}
	
	function registerOnChangeHide(){
		console.log("inside register on change....");
		$('.bussProgramHide').change(function (){
			var currentId = $(this).attr('id');
			if(this.checked){
				updateShowHideBusinessProgram(currentId,true);
			} else {
				updateShowHideBusinessProgram(currentId,false);
			}
		});
	}
	function registerOnChangeHealthy(){
		console.log("inside register on change....");
		$('.bussProgramHealthy').change(function (){
			var currentId = $(this).attr('id');
			if(this.checked){
				updateHealthy(currentId,true);
			} else {
				updateHealthy(currentId,false);
			}
		});
	}
	
	function registerOnChange(){
		$('.bussProgramRename').keypress(function (e){	
			console.log("in here....");
			if (e.which == 13) {
				e.preventDefault();
				var currentId = $(this).attr('id');
				updateBusinessProgram(currentId);
				return false; 
			}
		});
	}
	
	function updateHealthy(currentId,health){
		var packageName = $('#'+currentId).data(currentId);
		var idArray = currentId.split("_");
		console.log("current id "+currentId);
		console.log("life "+idArray[1]);
		console.log("packageName "+packageName);
		console.log("health"+health);
		
		$.getJSON("api/report/saveHealthMessage", {	
			"packageName":packageName,
			"life":idArray[1],
			"saveHealthMessage":health
		}) 
		.success(function(loadJson) {
			alertify.log("Health status updated.");
		})
		
	}
	
	function updateShowHideBusinessProgram(currentId,hidden){
		var packageName = $('#'+currentId).data(currentId);
		$.getJSON("api/report/saveShowHide", {	
			"packageName":packageName,
			"isHidden":hidden
		}) 
		.success(function(loadJson) {
			alertify.log("Show/Hide updated.");
		})
	}
	
	function updateBusinessProgram(currentId){
		//alertify.log(currentId);
		var packageName = $('#'+currentId).data(currentId);
		$.getJSON("api/report/saveDisplayName", {	
			"packageName":packageName,
			"displayName":$('#'+currentId).val()
		}) 
		.success(function(loadJson) {
			alertify.log("Display name updated.");
		})
	}
	
	
	
}
