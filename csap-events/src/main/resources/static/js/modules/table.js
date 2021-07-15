


define( [ "admin", "search", "health", "logrotate", "host" ], function ( adminModule, searchModule, healthModule, logRotateModule, hostModule ) {

    console.log( "Event module loaded" ) ;

    var csapUser = new CsapUser() ;

    var eventCounterTimer ;
    var countTimeout = false ;
    var FLAG_FOR_LOADING_COUNT = "-1" ;
    return {
        //
        initialize: function () {
            initialize() ;
        },
        //
        getFilteredRecordCount: function () {
            return getFilteredRecordCount() ;
        }
    }


    function initialize() {

        // Get min and max from DB
        $.when( parseQueryParams() )
                .then( refreshEventsTable )
                .then( function () {
                    searchModule.initFilters( scheduleFilterdRecordCount )
                } ) ;

    }


    function refreshEventsTable() {
        var refreshDeffered = $.Deferred() ;
        console.log( 'table refresh started' ) ;
        $( 'body' ).css( 'cursor', 'wait' ) ;
        var defaultDisplayLength = 20 ;
        var projectParameter = '' ;
        projectParameter = getParameterByName( "project" ) ;
        var categoryParameter = '' ;
        categoryParameter = getParameterByName( "category" ) ;
        var dateParameter = '' ;
        dateParameter = getParameterByName( "date" ) ;

        if ( projectParameter != '' && dateParameter != '' && categoryParameter == '/csap/reports/health' ) {
            defaultDisplayLength = 1000 ;
        }
        if ( projectParameter != '' && dateParameter != '' && categoryParameter == '/csap/reports/host/daily' ) {
            defaultDisplayLength = 50 ;
        }

        // https://datatables.net/manual/server-side
        var eventsDataTable = $( '#eventsTable' ).DataTable( {
            "processing": true,
            "autoWidth": false,
            "serverSide": true,
            "searching": true,
            "ordering": false,
            "lengthMenu": [ [ 10, 20, 50, 100, 200, 500, 1000 ],
                [ 10, 20, 50, 100, 200, 500, 1000 ] ],
            "displayLength": defaultDisplayLength,
            "oLanguage": {
                "sProcessing": '<div class="loadingLargePanel"> Retrieving Matching  Events </div>'
            },
            "ajax": {
                "url": api + "/event",
                "data": function ( d ) {
                    d.searchText = searchModule.constructSearchText() ;
                }
            },
            "columns": getEventColumns()
                    /*
                     * , "order" : [ [ 1, 'asc' ] ]
                     */

        } ) ;
        eventsDataTable.on( 'init.dt', function ( e, settings, json ) {
            //console.log("json"+JSON.stringify(json));

        } ) ;

        eventsDataTable.on( 'draw', function () {
            //console.log("user ids " + $(".userids").size());
            csapUser.onHover( $( ".userids" ), 500 ) ;
            //console.log("drawing")
            scheduleFilterdRecordCount() ;
        } ) ;
        eventsDataTable.on( 'page.dt', function () {
            //scheduleFilterdRecordCount();
        } ) ;
        eventsDataTable.on( 'length.dt', function () {
            console.log( "Page length changed" ) ;
            $( "#healthReportsTable" ).css( "display", "none" ) ;
            $( ".logRotateClass" ).hide() ;
            //scheduleFilterdRecordCount();
        } ) ;
        eventsDataTable.on( 'xhr', function () {
            var json = eventsDataTable.ajax.json() ;
            //console.log( 'Json' +JSON.stringify(json) );
            //from json populate health table if it is health report category
            var params = eventsDataTable.ajax.params() ;
            // console.log("params"+JSON.stringify(params));
            var searchString = params.searchText ;
            if ( searchString.indexOf( "/csap/reports/health" ) > 0 ) {
                console.log( "report health is category" ) ;
                healthModule.constructHealthTable( json ) ;
                //retrieveHealthData();
            } else {
                $( "#healthReportsTable" ).css( "display", "none" ) ;
            }
            if ( searchString.indexOf( "/csap/reports/logRotate" ) > 0 ) {
                logRotateModule.constructLogRotateTable() ;
            } else {
                //$("#logRotateTable").css("display","none");
                $( ".logRotateClass" ).hide() ;
            }
            if ( searchString.indexOf( "/csap/reports/host/daily" ) > 0 ) {
                hostModule.createHostTable( json ) ;
            } else {
                //$("#logRotateTable").css("display","none");
                $( ".hostClass" ).hide() ;
            }
        } ) ;

        $( '#eventsTable tbody' ).on( 'click', 'td.details-control', function () {
            var tr = $( this ).closest( 'tr' ) ;
            var row = eventsDataTable.row( tr ) ;
            if ( row.child.isShown() ) {
                row.child.hide() ;
                tr.removeClass( 'shown' ) ;
            } else {
                row.child( format( row.data() ) ).show() ;
                tr.addClass( 'shown' ) ;
            }
        } ) ;

        $( 'body' ).css( 'cursor', 'default' ) ;
        console.log( 'table refresh done' ) ;
        setTimeout( function () {
            console.log( 'loading table done' ) ;
            refreshDeffered.resolve() ;
        }, 250 ) ;
        return refreshDeffered ;
    }

    function getEventColumns() {
        var eventColumns = [
            {
                "data": "category"

            },
            {
                "data": "summary",
                "class": "col2 details-control",
                "render": function ( data, type, full, meta ) {
                    var summaryText = "" ;
                    if ( full.metaData && full.metaData.uiUser ) {
                        if ( 'System' != full.metaData.uiUser && 'agentUser' != full.metaData.uiUser ) {
                            summaryText = "<span class='userids'>"
                                    + full.metaData.uiUser
                                    + "</span>" ;
                        }
                    }
                    summaryText = summaryText + data ;
                    return summaryText ;
                }
            },
            {
                "data": "lifecycle"
            },
            {
                "data": "project"
            },
            {
                "data": "host",
                "render": function ( data, type, full, meta ) {
                    var hostName = data ;
                    var urlAction = agentHostUrlPattern.replace( /CSAP_HOST/g, hostName ) ;
                    var hostHref = '<a target="_blank" class="simple" href="' + urlAction + '/os/HostDashboard">' + data + "</a>" ;
                    return hostHref ;
                }
            },
            {
                "data": "createdOn.date",
                "render": function ( data, type, full, meta ) {
                    var date = data.substring( 5 ) ;
                    var time = full.createdOn.time ;
                    return date + " - " + time ;
                }
            },
            {
                "data": "createdOn.lastUpdatedOn.$date",
                "visible": false,
                "render": function ( data, type, full, meta ) {
                    var isDisplay = $( '#eventTimeStamp' ).prop( 'checked' ) ;
                    // console.log("rendering lastUpdatedOn: " + isDisplay, meta) ;
                    if ( data != null ) {
                        var datePart1 = data.substring( 5, 10 ) ;
                        var datePart2 = data.substring( 11, 19 ) ;
                        return datePart1 + " - " + datePart2
                                + " GMT" ;
                    } else {
                        return "" ;
                    }
                }
            }

        ]

        return eventColumns ;
    }

    function scheduleFilterdRecordCount() {

        var $dataTablesCountContainer = $( '#eventsTable_info' ) ;
        var oldhtml = $dataTablesCountContainer.html() ;
        console.log( "scheduleFilterdRecordCount2() table location: ", oldhtml ) ;
        if ( oldhtml.indexOf( FLAG_FOR_LOADING_COUNT ) == -1 ) {

            if ( countTimeout ) {
                var warning = '<img id="loadCountImage" width="14" src="'
                        + imagePath + '/16x16/warning.png"/>' ;
                $dataTablesCountContainer.html( $( '#eventsTable_info' ).html() + warning ) ;
                $dataTablesCountContainer.qtip( {
                    content: {
                        text: "Actual counts timed out. Reduce the date range for accurate results"
                    },
                    style: {
                        classes: 'qtip-bootstrap'
                    }
                } ) ;

            }
            return ;
        }
        console.log( "scheduleFilterdRecordCount() Updating: ", oldhtml, " with img" ) ;
        var loadingImage = '<img title="Querying Events for counts" id="loadCountImage" width="14" src="' + imagePath + '/animated/loadSmall.gif"/>' ;

        var findFlagRegex = new RegExp( FLAG_FOR_LOADING_COUNT, "g" ) ;
        var newhtml = oldhtml.replace( findFlagRegex, loadingImage ) ;
        // console.log("updated: ", newhtml) ;
        $( '#eventsTable_info' ).html( newhtml ) ;

        clearTimeout( eventCounterTimer ) ;

        if ( $( "#loadCountImage" ).length == 0 ) {
            console.log( "Only doing once" ) ;
            //eventCounterTimer = setTimeout( scheduleFilterdRecordCount, 1000 );
        } else {
            console.log( "Loading of events done. Updating count." ) ;
            clearTimeout( eventCounterTimer ) ;
            getFilteredCount() ;
        }
    }
    function buildAdminEditOptions( eventId, category ) {
        var editButton = jQuery( '<button/>', {
            class: "pushButton",
            title: "View/Edit event data",
            html: "<img src='" + imagePath + "/16x16/edit-select-all.png'>"


        } ).click( function () {
            adminModule.editEvent( eventId, isAuthorized, category ) ;
        } ) ;
        return editButton ;

    }
    function buildAdminDeleteOptions( eventId ) {
        var deleteButton = jQuery( '<button/>', {
            class: "pushButton",
            html: "<img src='" + imagePath + "/16x16/deleteFolder.png'>"

        } ).click( function () {
            adminModule.deleteEvent( eventId ) ;
        } ) ;
        return deleteButton ;
    }
    function buildViewOptions( eventId ) {
        var eventPath = eventApi + '/getById?id=' + eventId ;
        //var imgPath = "<img src='"+ conPath + "/images/generic.png'>";
        //var viewEvent = "<a href="+eventPath + " target='_blank'>"+ imgPath + " </a>";
        var viewButton = jQuery( '<button/>', {
            class: "pushButton",
            title: "Open event in browser",
            html: "<img src='" + imagePath + "/16x16/document-new.png'>"

        } ).click( function () {
            //document.location.href = eventPath;
            window.open( eventPath ) ;
        } ) ;

        return viewButton ;
    }

    function format( eventRowJQ ) {
        var tableContent = "" ;
        var objId = "" + eventRowJQ._id.$oid ;
        var eventContentDiv = jQuery( '<div/>', { } ) ;
        var eventContentTable = jQuery( '<table/>', {
            cellpadding: "5",
            cellspacing: "0",
            border: "0"
        } ).css( "padding-left", "50px" ) ;
        eventContentDiv.append( eventContentTable ) ;

        var idRow = jQuery( '<tr/>', { } ) ;
        idRow.append( jQuery( '<td/>', {
            text: "Id:"
        } ) ) ;
        var idCell = jQuery( '<td/>', { } ) ;
        if ( isAuthorized ) {
            idCell.append( buildAdminDeleteOptions( eventRowJQ._id.$oid ) ) ;
        }
        idCell.append( eventRowJQ._id.$oid ) ;
        idRow.append( idCell ) ;

        eventContentTable.append( idRow ) ;

        if ( eventRowJQ.appId ) {
            var appIdRow = jQuery( '<tr/>', { } ) ;
            appIdRow.append( jQuery( '<td/>', {
                text: "AppId:"
            } ) )
                    .append( jQuery( '<td/>', {
                        text: eventRowJQ.appId
                    } ) ) ;
            eventContentTable.append( appIdRow ) ;
        }
        if ( eventRowJQ.counter ) {
            var counterRow = jQuery( '<tr/>', { } ) ;
            counterRow.append( jQuery( '<td/>', {
                text: "Counter:"
            } ) )
                    .append( jQuery( '<td/>', {
                        text: eventRowJQ.counter
                    } ) ) ;
            eventContentTable.append( counterRow ) ;
        }


        var dateRow = jQuery( '<tr/>', { } ) ;
        dateRow.append( jQuery( '<td/>', {
            text: "Creation Date Time:"
        } ) )
                .append( jQuery( '<td/>', {
                    html: eventRowJQ.createdOn.date + ' - ' + eventRowJQ.createdOn.time + '<span class="ms">' + eventRowJQ.createdOn.unixMs + ' ms </span>'
                } ) ) ;
        eventContentTable.append( dateRow ) ;
        if ( eventRowJQ.expiresAt ) {
            var expiresRow = jQuery( '<tr/>', { } ) ;
            expiresRow.append( jQuery( '<td/>', {
                text: "Expires At:"
            } ) )
                    .append( jQuery( '<td/>', {
                        text: eventRowJQ.expiresAt.$date
                    } ) ) ;
            eventContentTable.append( expiresRow ) ;
        }
        if ( eventRowJQ.metaData ) {
            var metaDataRow = jQuery( '<tr/>', { } ) ;
            metaDataRow.append( jQuery( '<td/>', {
                text: "Meta Data:"
            } ) )
                    .append( jQuery( '<td/>', {
                        html: '<pre>' + JSON.stringify( eventRowJQ.metaData, null, "\t" ) + '</pre>'
                    } ) ) ;
            eventContentTable.append( metaDataRow ) ;
        }
        var dataObject = getDataById( eventRowJQ._id.$oid ) ;
        //console.log(dataObject);
        if ( dataObject.data && dataObject.data.csapText ) {
            console.log( "csap text" ) ;
            var csapTextRow = jQuery( '<tr/>', { } ) ;
            csapTextRow.append( jQuery( '<td/>', {
                text: "Data:"
            } ) ) ;
            var csapTextCell = jQuery( '<td/>', { } ) ;

            csapTextCell.append( buildAdminEditOptions( eventRowJQ._id.$oid, eventRowJQ.category ) ) ;

            csapTextCell.append( buildViewOptions( eventRowJQ._id.$oid ) ) ;
            csapTextCell.append( '<pre>' + dataObject.data.csapText + '</pre>' ) ;
            csapTextRow.append( csapTextCell ) ;
            eventContentTable.append( csapTextRow ) ;
        } else {
            console.log( "csap data" ) ;
            var dataRow = jQuery( '<tr/>', { } ) ;
            dataRow.append( jQuery( '<td/>', {
                text: "Data:"
            } ) ) ;
            var dataCell = jQuery( '<td/>', { } ) ;
            //if(isAuthorized && eventRowJQ.category.indexOf("/csap/metrics") < 0){
            //dataCell.append(buildAdminEditOptions(eventRowJQ._id.$oid));
            //}
            dataCell.append( buildAdminEditOptions( eventRowJQ._id.$oid, eventRowJQ.category ) ) ;
            dataCell.append( buildViewOptions( eventRowJQ._id.$oid ) ) ;
            dataCell.append( '<pre>' + JSON.stringify( dataObject, null, "\t" ) + '</pre>' ) ;
            dataRow.append( dataCell ) ;
            eventContentTable.append( dataRow ) ;
        }
        return eventContentDiv ;
    }

    function getDataById( objectId ) {
        var clusterDef = "" ;
        $.ajax( {
            url: api + "/event/data/" + objectId,
            dataType: 'json',
            async: false,
            success: function ( loadJson ) {
                clusterDef = loadJson ;
            }
        } ) ;
        return clusterDef ;
    }
    this.refreshPage = function () {
        searchModule.search() ;
    }


    function getFilteredRecordCount() {
        var filteredRecordCount ;
        var searchQuery = searchModule.constructSearchText() ;
        $.ajax( {
            url: api + "/event/filteredCount",
            data: {
                searchText: searchQuery
            },
            dataType: 'json',
            async: false,
            success: function ( loadJson ) {
                filteredRecordCount = loadJson.recordsFiltered ;
            }
        } ) ;
        return filteredRecordCount ;
    }

    function getFilteredCount() {
        var params = {
            searchText: searchModule.constructSearchText()
        }
        countTimeout = false ;

        $.getJSON(
                api + "/event/filteredCount", params )

                .done( function ( serverResponsForCount ) {
                    console.log( "updating filtered count" ) ;

                    var numMatches = 12345 ;
                    if ( serverResponsForCount.success ) {
                        numMatches = serverResponsForCount.recordsFiltered ;
                    } else {
                        countTimeout = true ;
                    }

                    $( '#eventsTable' )
                            .dataTable()
                            .api()
                            .ajax
                            .json()
                            .recordsFiltered = numMatches ;

                    var newCount = $( '#eventsTable' ).dataTable().api().ajax
                            .json().recordsFiltered ;

                    console.log( "Json new count::" + newCount ) ;


                    $( '#eventsTable' ).dataTable().api().csapDraw() ;

                    $( '#dbSize' ).remove() ;
                    var newhtml = $( '#eventsTable_info' ).html() ;
                    var sizeInMB = serverResponsForCount.dataSize / ( 1024 * 1024 ) ;
                    newhtml = newhtml
                            + " <span style='font-size: 0.7em;' id='dbSize'>( Data Size "
                            + precise_round( sizeInMB, 2 ) + " MB)</span>" ;
                    $( '#eventsTable_info' ).html( newhtml ) ;

                    console.log( "Updating record count done." ) ;
                    //}, 250);
                } )

                .fail(
                        function ( jqXHR, textStatus, errorThrown ) {
                            console.log( "Failed to get filter fields: ", jqXHR.status, errorThrown )

                            handleConnectionError(
                                    "Failed getting metadata: <br/>"
                                    + errorThrown, errorThrown ) ;
                        } ) ;

    }

    function handleConnectionError( command, errorThrown ) {

        console.log( command, errorThrown )

        if ( errorThrown == "abort" ) {
            console.log( "Request was aborted: " + command ) ;
            return ;
        }
        var message = "Failed connecting to server - this is most ofter triggered by SSO credential expiring." ;
        message += "<br><br>Command: " + command
        message += '<br><br>Server Response:<pre class="error" >' + errorThrown + "</pre>" ;

        var errorDialog = alertify.confirm( message ) ;

        errorDialog.setting( {
            title: "Host Connection Error",
            resizable: false,
            'labels': {
                ok: 'Reload Page',
                cancel: 'Ignore Error'
            },
            'onok': function () {
                document.location.reload( true ) ;
            },
            'oncancel': function () {
                alertify.warning( "Wait a few seconds and try again," ) ;
            }

        } ) ;

        $( 'body' ).css( 'cursor', 'default' ) ;
    }


    function parseQueryParams() {
        var r = $.Deferred() ;
        var categoryParameter = getParameterByName( "category" ) ;
        var jvmNameParameter = getParameterByName( "jvmName" ) ;
        var lifecycleParameter = getParameterByName( "life" ) ;
        var appIdParameter = getParameterByName( "appId" ) ;
        var dateParameter = getParameterByName( "date" ) ;
        var daysParameter = getParameterByName( "days" ) ;
        var projectNameParameter = getParameterByName( "project" ) ;
        var numDays ;
        if ( dateParameter != '' ) {
            try {
                var parsedDate = new Date( Number( dateParameter ) ) ;
                var dateObj = ( parsedDate.getMonth() + 1 ) + '/' + parsedDate.getDate() + '/' + parsedDate.getFullYear() ;
                //console.log("fromDate "+dateObj);
                $( "#from" ).val( dateObj ) ;
                $( "#to" ).val( dateObj ) ;
                numDays = 1 ;
            } catch ( err ) {
                console.log( err ) ;
                $( "#from" ).val( "" ) ;
                $( "#to" ).val( "" ) ;
                numDays = 0 ;
            }
        } else if ( daysParameter != '' ) {
            var toDate = new Date() ;
            var dateObj = ( toDate.getMonth() + 1 ) + '/' + toDate.getDate() + '/' + toDate.getFullYear() ;
            $( "#to" ).val( dateObj ) ;

            var fromDate = new Date() ;
            fromDate.setDate( toDate.getDate() - daysParameter )
            console.log( "fromDate", fromDate ) ;
            var fromObj = ( fromDate.getMonth() + 1 ) + '/' + fromDate.getDate() + '/' + fromDate.getFullYear() ;
            $( "#from" ).val( fromObj ) ;
            numDays = daysParameter ;
        } else {
            var toTime = new Date() ;
            var fromTime = new Date( toTime.getTime() - ( 7 * 24 * 60 * 60 * 1000 ) ) ;
            var toTimeStr = ( toTime.getMonth() + 1 ) + '/' + toTime.getDate() + '/' + toTime.getFullYear() ;
            var fromTimeStr = ( fromTime.getMonth() + 1 ) + '/' + fromTime.getDate() + '/' + fromTime.getFullYear() ;
            console.log( "toTimeStr " + toTimeStr ) ;
            console.log( "fromTimeStr " + fromTimeStr ) ;
            $( "#from" ).val( fromTimeStr ) ;
            $( "#to" ).val( toTimeStr ) ;
            numDays = 7 ;
        }

        if ( jvmNameParameter != '' ) {
            categoryParameter = categoryParameter + '/' + jvmNameParameter ;
        }
        addAllToDropDown( 'appIdSearch' ) ;
        addAllToDropDown( 'lifecycleSearch' ) ;
        addAllToDropDown( 'userIdSelect' ) ;
        addAllToDropDown( 'hostSelect' ) ;
        addAllToDropDown( 'projectSelect' ) ;
        if ( appIdParameter != '' ) {
            var optionContent = '<option selected="selected">' + appIdParameter + '</option>' ;
            $( '#appIdSearch' ).append( optionContent ) ;
        }
        if ( lifecycleParameter != '' ) {
            var optionContent = '<option selected="selected">' + lifecycleParameter + '</option>' ;
            $( '#lifecycleSearch' ).append( optionContent ) ;
        }
        if ( projectNameParameter != '' ) {
            var optionContent = '<option selected="selected">' + projectNameParameter + '</option>' ;
            $( '#projectSelect' ).append( optionContent ) ;
        }
        $( "#simpleSearch" ).val( categoryParameter ) ;

        var categoryDisplayText = categoryParameter ;
        if ( numDays > 0 ) {
            if ( categoryDisplayText != '' ) {
                categoryDisplayText = categoryDisplayText + ":" ;
            }
            categoryDisplayText = categoryDisplayText + numDays + " day(s)"
        }

        searchModule.addLaunchParameters() ;

        $( "#categoryInput" ).text( categoryDisplayText ) ;
        $( "#summarySearch" ).val( "" ) ;
        $( '#eventTimeStamp' ).attr( 'checked', false ) ;
        setTimeout( function () {
            console.log( 'parsing query parameters done' ) ;
            r.resolve() ;
        }, 250 ) ;
        return r ;
    }



    function addAllToDropDown( id ) {
        $( '#' + id ).empty() ;
        $( '#' + id ).append( '<option value="">All</option>' ) ;
    }


} ) ;
