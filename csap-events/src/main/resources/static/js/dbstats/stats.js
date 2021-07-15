$( document ).ready( function () {

	var stats = new Stats();
	CsapCommon.configureCsapAlertify();
	stats.appInit();

} );
function Stats() {

	this.appInit = function () {
		console.log( "Init in stats" );

		$( '#dbSelect' ).change( function () {
			loadAll();
		} );

		$( '#primaryStepDown' ).click( function () {
			$.getJSON( api + "/admin/replicaStepDown", {
			} )
					.done( function ( loadJson ) {
						alertify.notify( "result" +  JSON.stringify( loadJson, null, "\t"  ));

						setTimeout( function () {
							loadAll();
						}, 500 );

					} );
		} );
		$( '#colSelect' ).change( function () {
			loadCollectionStats();
		} );
		loadAll();

	};

	function loadAll() {

		$( "tbody" ).html( $( "#loadingTemplate" ).html() );
		//loadDbNames().done(loadCollectionNames);
		$.when( loadDbNames() ).then( loadCollectionNames ).then( loadDbStats ).then( loadCollectionStats );
		loadMongoOptions();
	}
//	function loadCollectionNameDbStatsAndCollectionStats() {
//		$.when( loadCollectionNames() ).then( loadDbStats ).then( loadCollectionStats );
//	}


	function loadDbStats() {
		$( 'body' ).css( 'cursor', 'wait' );
		var r = $.Deferred();
		$.getJSON( api + "/admin/dbStatus", {
			"dbname": $( '#dbSelect' ).val()
		} )
				.done( function ( loadJson ) {
					loadDbStatsSuccess( loadJson );
				} );
		setTimeout( function () {
			console.log( 'loading db stats done' );
			r.resolve();
		}, 500 );
		return r;
	}

	function loadCollectionStats() {
		$( 'body' ).css( 'cursor', 'wait' );
		$( "#collStatusBody" ).html( $( "#loadingTemplate" ).html() );
		
		var r = $.Deferred();
		$.getJSON( api + "/admin/collStatus", {
			"dbname": $( '#dbSelect' ).val(),
			"collName": $( '#colSelect' ).val()
		} )
				.done( function ( loadJson ) {
					loadCollectionStatsSuccess( loadJson );
				} );
		setTimeout( function () {
			console.log( 'loading coll stats done' );
			r.resolve();
		}, 500 );
		return r;
	}

	function loadCollectionNames() {
		var r = $.Deferred();
		$.getJSON( api + "/admin/colls", {
			"dbname": $( '#dbSelect' ).val()
		} )
				.done( function ( loadJson ) {
					loadCollectionNameSuccess( loadJson );
				} );
		setTimeout( function () {
			console.log( 'loading coll done' );
			r.resolve();
		}, 500 );
		return r;
	}

	function loadDbNames() {
		var r = $.Deferred();
		$.ajax( {
			url: api + "/admin/dbs",
			dataType: 'json',
			async: false,
			success: function ( loadJson ) {
				loadDbNameSuccess( loadJson );
			}
		} );
		/*
		 $.getJSON("api/admin/dbs", {			
		 }) 
		 .success(function(loadJson) {			
		 loadDbNameSuccess(loadJson);
		 });
		 */
		setTimeout( function () {
			console.log( 'loading dbs done' );
			r.resolve();
		}, 500 );
		return r.promise();
	}

	function loadMongoOptions() {
		$.getJSON( api + "/admin/mongoOptions", {
		} )
				.done( function ( loadJson ) {
					loadMongoOptionsSuccess( loadJson );
				} );
	}

	function loadDbNameSuccess( dataJson ) {
		$( '#dbSelect' ).empty();
		for ( var i = 0; i < dataJson.length; i++ ) {
			//
			var optionContent;
			if ( dataJson[i] == 'event' ) {
				optionContent = '<option  selected="selected" value="' + dataJson[i] + '">' + dataJson[i] + '</option>';
			} else {
				optionContent = '<option value="' + dataJson[i] + '">' + dataJson[i] + '</option>';
			}
			$( '#dbSelect' ).append( optionContent );
		}
	}

	function loadCollectionNameSuccess( dataJson ) {
		$( '#colSelect' ).empty();
		for ( var i = 0; i < dataJson.length; i++ ) {
			var optionContent;
			optionContent = '<option value="' + dataJson[i] + '">' + dataJson[i] + '</option>';
			$( '#colSelect' ).append( optionContent );
		}
	}
	function loadDbStatsSuccess( dataJson ) {
		$( '#dbName' ).empty();
		$( '#dbName' ).append( '( ' + $( '#dbSelect' ).val() + ' )' );
		updateTable( dataJson, "dbStatusTable", "dbStatusBody" );
	}
	function loadCollectionStatsSuccess( dataJson ) {
		$( '#collectionName' ).empty();
		$( '#collectionName' ).append( '( ' + $( '#colSelect' ).val() + ' )' );
		updateTable( dataJson, "collStatusTable", "collStatusBody" );
	}

	function updateTable( dataJson, tableId, tableBodyId ) {
		$( "#" + tableBodyId ).empty();
		var primaryServer;
		var secondaryServer;
		for ( var i = 0; i < dataJson.length; i++ ) {
			if ( dataJson[i].key == 'serverUsed' ) {
				primaryServer = dataJson[i].primaryValue;
				secondaryServer = dataJson[i].secondaryValue;
			}
			var collStatusTrContent = '<td style="font-size: 1.0em" > ' + getKey( dataJson[i].key ) + '</td>' +
					'<td style="font-size: 1.0em" > <div>' + statusCheck( dataJson, i ) + convertSizeToMB( dataJson[i].key, dataJson[i].primaryValue ) + '</div></td>' +
					'<td style="font-size: 1.0em" > <div>' + statusCheck( dataJson, i ) + convertSizeToMB( dataJson[i].key, dataJson[i].secondaryValue ) + '</div></td>'
					;
			var collStatusTr = $( '<tr />', {
				'class': "",
				html: collStatusTrContent
			} );
			$( '#' + tableBodyId ).append( collStatusTr );
		}
		addResturl( primaryServer, secondaryServer, tableId, tableBodyId );
		$( "#" + tableId ).trigger( "update" );
		$( 'body' ).css( 'cursor', 'default' );
	}

	function statusCheck( dataJson, counter ) {
		var statusImage = "";
		if ( dataJson[counter].key == 'collections' || dataJson[counter].key == 'count' ) {
			if ( dataJson[counter].primaryValue == dataJson[counter].secondaryValue ) {
				statusImage = '<img src="images/correct.gif">';
			} else {
				statusImage = '<img src="images/warning.png">';
			}
		}
		return statusImage;
	}

	function getKey( key ) {
		if ( key.endsWith( 'Size' ) ) {
			return key + '( MB )';
		} else {
			return key;
		}
	}

	function convertSizeToMB( key, size ) {
		var convertedSize = size;
		if ( key.endsWith( 'Size' ) ) {
			var sizeInMB = (size / (1024 * 1024));
			convertedSize = precise_round( sizeInMB, 2 );
		}
		return convertedSize;

	}

	function precise_round( value, decPlaces ) {
		var val = value * Math.pow( 10, decPlaces );
		var fraction = (Math.round( (val - parseInt( val )) * 10 ) / 10);
		if ( fraction == -0.5 )
			fraction = -0.6;
		val = Math.round( parseInt( val ) + fraction ) / Math.pow( 10, decPlaces );
		return val;
	}

	function addResturl( primaryServer, secondaryServer, tableId, tableBodyId ) {

		var collStatusTrContent = '<td style="font-size: 1.0em" > ' + 'Mongo Rest Interface' + '</td>' +
				'<td style="font-size: 1.0em" > <a target="_blank" href="' + getRestUrl( primaryServer ) + '">' + "Click Here" + '</td>' +
				'<td style="font-size: 1.0em" > <a target="_blank" href="' + getRestUrl( secondaryServer ) + '">' + "Click Here" + '</td>'
				;
		var collStatusTr = $( '<tr />', {
			'class': "",
			html: collStatusTrContent
		} );
		$( '#' + tableBodyId ).append( collStatusTr );

	}
	function getRestUrl( serverUsed ) {
		var urlarr = serverUsed.split( "/" );
		if ( urlarr.length == 1 )
			urlarr = serverUsed.split( ":" );
		return "http://" + urlarr[0] + ":28017";
	}

	function loadMongoOptionsSuccess( dataJson ) {

		$( "#mongoOptionsBody" ).empty();
		updateDriverInfo( dataJson );
		for ( var key in dataJson.mongoOptions ) {
			//console.log(key + "-->"+dataJson.mongoOptions[key] );
			var mongoOptionsTrContent = '<td style="font-size: 1.0em" > ' + key + '</td>' +
					'<td style="font-size: 1.0em" > ' + JSON.stringify( dataJson.mongoOptions[key] ) + '</td>'
					;
			var mongoOptionsTr = $( '<tr />', {
				'class': "",
				html: mongoOptionsTrContent
			} );
			$( '#mongoOptionsBody' ).append( mongoOptionsTr );
		}

		$( "#mongoOptionsTable" ).trigger( "update" );
		$( 'body' ).css( 'cursor', 'default' );
	}

	function updateDriverInfo( dataJson ) {
		var mongoOptionsTrContent = '<td style="font-size: 1.0em" > ' + 'Java Driver Version' + '</td>' +
				'<td style="font-size: 1.0em" > ' + dataJson.driverInfo + '</td>'
				;
		var mongoOptionsTr = $( '<tr />', {
			'class': "",
			html: mongoOptionsTrContent
		} );
		$( '#mongoOptionsBody' ).append( mongoOptionsTr );
	}



}
