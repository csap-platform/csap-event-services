// Note: circular dependency with table, so we use alternate include
define( [], function (  ) {

	console.log( "Search module loaded" );
	var refreshTable;

	var _updateTableFiltersFunction = null;

	return {
		//
		initFilters: function ( updateTableFiltersFunction ) {
			_updateTableFiltersFunction = updateTableFiltersFunction;
			initFilters();
		},
		//
		search: function () {
			return search();
		},
		//
		addLaunchParameters: function() {
			addLaunchParameters();
		},
		//
		constructSearchText: function () {
			return constructSearchText();
		},
		//
		showDialog: function () {
			showDialog()
		},
		//
		initialize: function () {
			initialize()
		}

	}

	function initialize() {
		datePickerInit();

		$( "#searchButton" ).click( function () {
			searchSetup();
		} );

		$( "#simpleSearch" ).keyup( function ( e ) {
			if ( e.which == 13 ) { // enter key
				var diffDays = getDateDiff();
				var categoryInputText = $( "#simpleSearch" ).val();
				if ( diffDays > 0 ) {
					if ( categoryInputText != '' ) {
						categoryInputText = categoryInputText + ":";
					}
					categoryInputText = categoryInputText + diffDays + " day(s)";
				}
				$( "#categoryInput" ).text( categoryInputText );
				search().done( _updateTableFiltersFunction );
				alertify.closeAll();
			}
		} );

		$( '#searchDialog' ).click( function () {
			showDialog();
			return false;
		} );

		$( ".advancedFilters" ).change( function () {
			var currentId = this.id;
			console.log( "Inside change filter" );
			refreshSearchFilters();
		} );
		$( "#autoRefresh" ).change( function () {
			var autoRefreshRate = $( '#autoRefresh' ).val();
			if ( autoRefreshRate > 0 ) {
				clearTimeout( refreshTable );
				autoRefreshTable( autoRefreshRate );
			} else {
				clearTimeout( refreshTable );
			}
		} );
	}
	function getDateDiff() {

		var fromDate = $( "#from" ).val();
		var toDate = $( "#to" ).val();
		if ( fromDate != '' && toDate != '' ) {
			var startDate = new Date( fromDate );
			var endDate = new Date( toDate );
			var timeDiff = Math.abs( startDate.getTime() - endDate.getTime() );
			var diffDays = Math.ceil( timeDiff / (1000 * 3600 * 24) );
			return diffDays + 1;
		}
		return 0;

	}
	function datePickerInit() {
		$( "#from" ).datepicker( {
			defaultDate: "+0w",
			changeMonth: true,
			numberOfMonths: 1,
			onClose: function ( selectedDate ) {
				var toDateVal = $( "#to" ).val();
				if ( toDateVal == '' ) {
					$( "#to" ).val( selectedDate );
				}
				$( "#to" ).datepicker( "option", "minDate", selectedDate );
				searchSetup();
			}
		} );
		$( "#to" ).datepicker( {
			defaultDate: "+0w",
			changeMonth: true,
			numberOfMonths: 1,
			onClose: function ( selectedDate ) {
				var fromDateVal = $( "#from" ).val();
				if ( fromDateVal == '' ) {
					$( "#from" ).val( selectedDate );
				}
				$( "#from" ).datepicker( "option", "maxDate", selectedDate );
				searchSetup();
			}
		} );

	}

	function searchSetup() {
		var diffDays = getDateDiff();
		var categoryInputText = $( "#simpleSearch" ).val();
		if ( diffDays > 0 ) {
			if ( categoryInputText != '' ) {
				categoryInputText = categoryInputText + ":";
			}
			categoryInputText = categoryInputText + diffDays + " day(s)";
		}
		$( "#categoryInput" ).text( categoryInputText );
		search().done( _updateTableFiltersFunction );
	}

	function settingsFactory() {
		return{
			build: function () {
				// Move content from template
				this.setContent( $( "#advanced" ).show()[0] );
				this.setting( {
					'onok': searchSetup,
					'oncancel': function () {
						//  alertify.notify( "Closing Windows" );

					}
				} );
			},
			setup: function () {
				return {
					buttons: [
						{ text: "Perform Search", className: alertify.defaults.theme.ok },
						{ text: "Cancel", className: alertify.defaults.theme.cancel }
					],
					options: {
						title: "Search Criteria", resizable: false, movable: false, maximizable: false,
					}
				};
			}

		};
	}
	function showDialog() {
		if ( !alertify.searchSettings ) {
			alertify.dialog( 'searchSettings', settingsFactory, false, 'confirm' );
		}

		alertify.searchSettings().show();

	}

	function search() {

		var searchDeferred = $.Deferred();
		$( "#healthReportsTable" ).css( "display", "none" );
		$( ".logRotateClass" ).hide();
		var searchText = constructSearchText();
		var eventsDataTable = $( '#eventsTable' ).DataTable();
		if ( $( '#eventTimeStamp' ).prop( 'checked' ) ) {
			eventsDataTable.column( 6 ).visible( true );
		} else {
			eventsDataTable.column( 6 ).visible( false );
		}

		eventsDataTable.search( searchText );
		eventsDataTable.ajax.reload();


		setTimeout( function () {
			console.log( 'search completed' );
			refreshSearchFilters();
			searchDeferred.resolve();
		}, 250 );

		return searchDeferred;
	}

	function initFilters() {
		console.log( "Initializing search filters" );

		var r = $.Deferred();
		//console.log("Init filters in search module");
		var categoryParameter = getParameterByName( "category" );
		var jvmNameParameter = getParameterByName( "jvmName" );

		if ( jvmNameParameter != '' ) {
			categoryParameter = categoryParameter + '/' + jvmNameParameter;
		}

		refreshSearchFilters();

		$( "#simpleSearch" ).val( categoryParameter );
		//$("#from").val("");
		//$("#to").val("");
		$( "#summarySearch" ).val( "" );

		setTimeout( function () {
			console.log( 'loading filters done' );
			r.resolve();
		}, 500 );
		return r;
	}

	function refreshSearchFilters() {

		$( "#metaLoading" ).css("visibility", "visible");

		$( "#metaTimeOut" ).css("visibility", "hidden");

		var params = {
			appId: $( "#appIdSearch" ).val(),
			life: $( "#lifecycleSearch" ).val(),
			fromDate: $( "#from" ).val(),
			toDate: $( "#to" ).val()
		}

		console.log( "refreshing search filters", params );

		$.getJSON(
				api + "/event/metadata", params )

				.done( function ( loadJson ) {
					console.log( "updating search filters" );

					$( "#metaLoading" ).css("visibility", "hidden");
					populateSearchFilters( "lifecycleSearch", null, loadJson.lifecycles );
					populateSearchFilters( "appIdSearch", null, loadJson.appIds );
					populateSearchFilters( "projectSelect", null, loadJson.projects );
					populateSearchFilters( "userIdSelect", null, loadJson.uiUsers );
					populateSearchFilters( "hostSelect", null, loadJson.hosts );
					
					// some parameters may not be in current time interval;
					addLaunchParameters() ;
					
					if ( loadJson.categories.length == 0 ) {
						console.log( "refreshSearchFilters() Skipping categories because no items returned." );
					} else {
						console.log( "refreshSearchFilters() Adding categories: ", loadJson.categories.length );
						$( "#simpleSearch" ).autocomplete( {
							source: loadJson.categories
						} );
					}
					console.groupEnd( "Completed Application Model" );
				} )

				.fail(
						function ( jqXHR, textStatus, errorThrown ) {
							console.log( "Failed to get filter fields: ", jqXHR.status, errorThrown )

							handleConnectionError(
									"Failed getting metadata: <br/>"
									+ errorThrown, errorThrown );
						} );
	}

	function handleConnectionError( command, errorThrown ) {

		console.log( command, errorThrown )

		if ( errorThrown == "abort" ) {
			console.log( "Request was aborted: " + command );
			return;
		}
		var message = "Failed connecting to server - this is most ofter triggered by SSO credential expiring.";
		message += "<br><br>Command: " + command
		message += '<br><br>Server Response:<pre class="error" >' + errorThrown + "</pre>";

		var errorDialog = alertify.confirm( message );

		errorDialog.setting( {
			title: "Host Connection Error",
			resizable: false,
			'labels': {
				ok: 'Reload Page',
				cancel: 'Ignore Error'
			},
			'onok': function () {
				document.location.reload( true );
			},
			'oncancel': function () {
				alertify.warning( "Wait a few seconds and try again," );
			}

		} );

		$( 'body' ).css( 'cursor', 'default' );
	}

	function populateSearchFilters( id, parameterValue, dataArray ) {

		if ( dataArray.length == 0 ) {
			console.log( "populateSearchFilters() Skipping item because no items returned : " + id );
			$( "#metaTimeOut" ).css("visibility", "visible");
			return;
		}
		if ( parameterValue == null ) {
			parameterValue = $( '#' + id ).val();
		}

		console.log( "populateSearchFilters() Adding: " + id + " items: ", dataArray.length )

		$( '#' + id ).empty();
		$( '#' + id ).append( '<option value="">All</option>' );

		if ( dataArray == null ) {
			// initial rendering
			$( '#' + id ).append( '<option>' + parameterValue + '</option>' );
			return;
		}
		dataArray.sort( function ( a, b ) {
			if ( a != null && b != null ) {
				return a.toLowerCase().localeCompare( b.toLowerCase() );
			} else {
				return -1;
			}
		} );


		for ( var i = 0; i < dataArray.length; i++ ) {
			if ( dataArray[i] == null )
				continue;
			var optionContent = '<option';
			if ( dataArray[i].toUpperCase() == parameterValue.toUpperCase() ) {
				optionContent = optionContent + ' selected="selected" ';
			}
			optionContent = optionContent + '>' + dataArray[i] + '</option>';
			$( '#' + id ).append( optionContent );
		}
	}

	function addLaunchParameters() {
		

		var useridParameter = getParameterByName( "userid" );

		if ( useridParameter != '' ) {
			var $userSelect = $( '#userIdSelect' ) ;
			if ( ! optionExists($userSelect, useridParameter)) {
				console.log("adding: '" + useridParameter + "'") ;
				var optionContent = '<option>' + useridParameter + '</option>';
				$userSelect.append( optionContent );
			}
			$userSelect.val( useridParameter ) ;
		}
		

		var hostNameParameter = getParameterByName( "hostName" );
		if ( hostNameParameter != '' ) {
			var hostSelect = $( '#hostSelect' ) ;
			if ( ! optionExists(hostSelect, hostNameParameter)) {
				console.log("adding: '" + hostNameParameter + "'") ;
				var optionContent = '<option>' + hostNameParameter + '</option>';
				hostSelect.append( optionContent );
			}
			hostSelect.val( hostNameParameter ) ;
		}

	}
	
	function optionExists($select, val) {
		  return $("option", $select).filter(function() {
		           return this.value === val;
		         }).length !== 0;
	}

	function constructSearchText() {
		//console.log("In construct search text");
		var searchText = "";

		searchText = extractSearchTextFromUi( "appIdSearch", "appId", searchText );
		searchText = extractSearchTextFromUi( "projectSelect", "project",
				searchText );
		searchText = extractSearchTextFromUi( "lifecycleSearch", "lifecycle",
				searchText );
		searchText = extractSearchTextFromUi( "from", "from", searchText );
		searchText = extractSearchTextFromUi( "to", "to", searchText );
		searchText = extractSearchTextFromUi( "userIdSelect", "metaData.uiUser",
				searchText );
		searchText = extractSearchTextFromUi( "hostSelect", "host", searchText );
		searchText = extractSearchTextFromUi( "summarySearch", "summary",
				searchText );
		searchText = extractSearchTextFromUi( "simpleSearch",
				"simpleSearchText", searchText );
		if ( $( '#eventTimeStamp' ).prop( 'checked' ) ) {
			searchText = searchText + "," + "eventReceivedOn=true";
		} else {
			searchText = searchText + "," + "eventReceivedOn=false";
		}
		//searchText = searchText +","+"isDataRequired=false";

		if ( searchText.indexOf( "/csap/reports/health" ) > 0 || searchText.indexOf( "/csap/reports/host/daily" ) > 0 ) {
			searchText = searchText + "," + "isDataRequired=true";
		} else {
			searchText = searchText + "," + "isDataRequired=false";
		}

		console.log( "searchText::" + searchText );


		return searchText;
	}

	function extractSearchTextFromUi( id, queryParam, searchText ) {
		var searchValue = $( '#' + id ).val();
		if ( searchValue != "" && searchValue != null ) {
			searchText = searchText + addComma( searchText ) + queryParam + "="
					+ searchValue;
		}
		return searchText;
	}

	function addComma( searchText ) {
		if ( searchText != "" && searchText.length > 0 ) {
			return ",";
		} else {
			return "";
		}
	}

	function autoRefreshTable( autoRefreshRate ) {

		search()
				.done( _updateTableFiltersFunction );
		clearTimeout( refreshTable );
		refreshTable = setTimeout( autoRefreshTable, autoRefreshRate,
				autoRefreshRate );
	}



} );