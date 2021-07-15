define(["search","require"], function (searchModule,require) {

	 console.log("Admin module loaded");

	 return {
		  //
		  deleteEvent: function (objectId) {
				deleteEvent(objectId)
		  },
		  //
		  addEvent: function () {
				showDialog()
		  },
		  //
		  editEvent: function (objectId,isAuthorized,category) {
				//editEvent(objectId)
				showEditDialog(objectId,isAuthorized,category)
		  },
		  //
		  deleteByFilter: function () {
				deleteByFilter()
		  },
		  initialize : function(){
				initialize()
		  }
	 }
	 
	 function initialize(){
		 $("#adminButtons").insertAfter('.headDate') ;
		  $('#deleteButton').click(function () {
				 deleteByFilter();
		  });

		  $('#insertButton').click(function () {
				console.log("insert button clicked");
				showDialog();

		  });
	 }

	 function deleteByFilter() {
		  var filteredCount = require( "table" ).getFilteredRecordCount();
		  console.log(filteredCount);
		  //if(filteredCount > 10){
		  alertify.confirm("Do you want to delete " + filteredCount + " records ?", function (e) {
				if (e) {
					 var searchText = searchModule.constructSearchText();
					 
					 
					 $.ajax({
						  url: eventApi+"/deleteBySearch?searchString=" + searchText,
						  dataType: 'json',
						  async: false,
						  success: function (loadJson) {
								console.log(loadJson);
								alertify.notify(loadJson.result);
								searchModule.search();
						  }
					 });
				} else {
					 console.log("cancel");
				}
		  });
	 }
	 function deleteEvent(objectId) {
		  console.log(objectId);
		  alertify.confirm("Do you want to delete record ?", function (e) {
			  if (e) {
				  $.ajax({
						url: eventApi+"/delete?objectId=" + objectId,
						dataType: 'json',
						async: false,
						success: function (loadJson) {
							 console.log(loadJson);
							 alertify.notify(loadJson.result);
							 searchModule.search();
						}
				  });
			  }
		  });
		  //event.refreshPage();
		  
	 }
	 
	 function insertEvent(){
		  $.ajax({
						  type: "POST",
						  url: eventApi+"/insert",
						  dataType: 'json',
						  async: false,
						  data: {
								appId: $("#eventInsertappId").val(),
								life: $("#eventInsertLife").val(),
								project: $("#insertEventProject").val(),
								eventJson: $("#insertEventData").val(),
								summary: $("#insertEventSummary").val(),
								category: $("#insertEventCategory").val()
						  },
						  success: function (loadJson) {
								//console.log(loadJson);
								alertify.notify(loadJson.result);
						  }
					 });
	 }

	 function showDialog() {
		  if (!alertify.insertNewItem) {
				//console.log("&&&&&&&&"+$("#appIdSearch").val());
				$("#eventInsertappId").val($("#appIdSearch").val());
				$("#eventInsertLife").val($("#lifecycleSearch").val())
				$("#insertEventProject").val($("#projectSelect").val())
				$("#insertEventCategory").val($("#simpleSearch").val())
				$("#insertEventSummary").val($("#summarySearch").val())
				$("#insertEventData").css("width", "100%").css("height", Math.floor($(window).outerHeight(true) - 250) + "px");

				var settingsFactory = function factory() {
					 return{
						  build: function () {
								// Move content from template
								this.setContent($("#editDialog").show()[0]);

								this.setting({
									 'onok': function () {
										  //  alertify.notify( "Closing Windows" );
										  insertEvent();
										  searchModule.search();
									 },
									 'oncancel': function () {
										  //  alertify.notify( "Closing Windows" );

									 }
								});
						  },
						  setup: function () {
								return {
									 buttons: [{text: "Add Event", className: alertify.defaults.theme.ok},
										  {text: "Cancel", className: alertify.defaults.theme.cancel}
									 ],
									 options: {
										  title: "Insert new item", resizable: false, movable: false, maximizable: false,
									 }
								};
						  }

					 };
				};


				alertify.dialog('insertNewItem', settingsFactory, false, 'confirm');
		  }

		  alertify.insertNewItem().show();

	 }

	 function addEvent() {
		  console.log("Inside add event");
		  var alertifyContainer = jQuery('<div/>', {
				id: "alertifyContainerId"
		  });
		  $("#eventInsertappId").val($("#appIdSearch").val());
		  $("#eventInsertLife").val($("#lifecycleSearch").val())
		  $("#insertEventProject").val($("#projectSelect").val())


		  alertify.prompt("<div id='alertifyContainerId'> </div>", function (
					 e, str) {
				if (e) {
					 $.ajax({
						  type: "POST",
						  url: eventApi+"/insert",
						  dataType: 'json',
						  data: {
								appId: $("#eventInsertappId").val(),
								life: $("#eventInsertLife").val(),
								project: $("#insertEventProject").val(),
								eventJson: $("#insertEventData").val(),
								summary: $("#insertEventSummary").val(),
								category: $("#insertEventCategory").val()
						  },
						  async: false,
						  success: function (loadJson) {
								//console.log(loadJson);
								alertify.log(loadJson.result);
						  }
					 }); 

					 searchModule.search();

				}
				setTimeout(function () {
					 $("#editTemplate").append($("#editDialog"));
				}, 100)


		  }, "");
		  $("#insertEventData").css("width", "100%").css("height", Math.floor($(window).outerHeight(true) - 250) + "px");
		  $("#alertifyContainerId").append($("#editDialog"));
		  var width = Math.floor($(window).outerWidth(true) - 250);
		  $(".alertify").css("width", width + "px");

		  $(".alertify").css("margin-left", "-" + (width / 2) + "px");
		  $(".alertify-inner").css("text-align", "left");
		  //$(".alertify-inner").css("white-space", "pre-wrap");
		  $(".alertify-text").css("display", "none");

		  $("#alertify-ok").text("Submit");
		  $("#alertify-cancel").text("Cancel");
	 }
	 
	 
	 function showEditDialog(objectId,isAuthorized,category) {
		 console.log("category :: " + category);
		var csapData = retrieveEventById(objectId);
		$("#editEventData").val(csapData);
		  if (!alertify.editItem) {
				
				$("#editEventData").css("width", "100%").css("height", Math.floor($(window).outerHeight(true) - 250) + "px");
				
				var settingsFactory = function factory() {
					 return{
						  build: function () {
								// Move content from template
								this.setContent($("#eventEditDialog").show()[0]);

								this.setting(getShowEditHandler(isAuthorized,objectId,category));
						  },
						  setup: function () {
								return {
									 buttons: getEditCancelButtons(isAuthorized,category) ,
									 options: {
										  title: "Edit item", resizable: false, movable: false, maximizable: false,
									 }
								};
						  }

					 };
				};
				
				alertify.dialog('editItem', settingsFactory, false, 'confirm');
				
		  }

		  alertify.editItem().show();

	 }
	 
	 function getShowEditHandler(isAuthorized,objectId,category){
		 
		 var callback = {
			 'oncancel': function () {
				  //alertify.notify( "Closing Windows" );
			 }
		}
		 if(isAuthorized && category.indexOf("/csap/metrics") < 0){
			 
			 $.extend( callback, {
				 'onok' :  function () {
					  editEventById(objectId);
					  searchModule.search();
				 }
			 } );
			
		 }
		 
		 return callback;
	 }
	 
	 function getEditCancelButtons(isAuthorized,category){
		 var buttons;
		 if(isAuthorized && category.indexOf("/csap/metrics") < 0){
			 buttons = [{text: "Update Event", className: alertify.defaults.theme.ok},
						  {text: "Cancel", className: alertify.defaults.theme.cancel}];
		 } else {
			 buttons = [
						  {text: "Close", className: alertify.defaults.theme.cancel}];
		 }
		
		 return buttons;
		 
	 }
				 
	 function retrieveEventById(objectId){
		 var csapData = "";
		 $.ajax({
				url: api + "/event/data/" + objectId,
				dataType: 'json',
				async: false,
				success: function (loadJson) {
					 console.log(loadJson);
					 if (loadJson.data && loadJson.data.csapText) {
						  if (loadJson.data.csapText.indexOf("{") == 1) {
								csapData = csapData = JSON.stringify(loadJson.data.csapText, null, "\t");
								;
						  } else {
								csapData = loadJson.data.csapText
						  }
					 } else if (loadJson.data){
						  csapData = JSON.stringify(loadJson.data, null, "\t");
					 } else {
						 csapData = JSON.stringify(loadJson, null, "\t");
					 }
				}
		 });
		return csapData;
	 }

	 function editEventById(objectId) {
		 $.ajax({
			 type: "POST",
			 url: eventApi+"/update",
			 dataType: 'json',
			 data: {
				  objectId: objectId,
				  eventData: $("#editEventData").val()
			 },
			 async: false,
			 success: function (loadJson) {
				  console.log(loadJson);
				  alertify.notify(loadJson.result);
			 }
		 });

	}


});