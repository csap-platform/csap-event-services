$( document ).ready( function () {
    var instance = new Instance();
    instance.appInit();

} );
function Instance() {

    var $table = $( "#servicesTable" );

    var $tableHeadRow = $( "thead tr", $table );
    var $tableFootRow = $( "tfoot tr", $table );

    var $tableBody = $( "tbody", $table );

    var projName = getParameterByName( "projName" );

    this.appInit = function () {
        console.log( "Init in instance" );



    };
    loadInstanceInfo();
    var instanceMap;
    function loadInstanceInfo() {
        var r = $.Deferred();

        $( "#page-info", "body>header" ).html( `Service Templates: ${projName}` )
        var appId = getParameterByName( "appId" );


        $.getJSON( baseUrl + "api/report/package-summary", {
            "projectName": projName,
            "appId": appId
        } ).done( function ( loadJson ) {
            instanceInfoSuccess( loadJson );
        } );
        setTimeout( function () {
            console.log( 'loading health info  done' );
            r.resolve();
        }, 500 );
        return r;
    }

    function instanceInfoSuccess( dataJson ) {

        $tableBody.empty();
        instanceMap = new Object();

        var numLifecycles = 0;
        for ( var lifecycle in dataJson ) {
            numLifecycles++;
//			var $lifeColumn = jQuery( '<td/>', { text: $( this ).data( "name" ) } );
            var $lifeColumn = jQuery( '<th/>', {text: lifecycle} );

            $tableHeadRow.append( $lifeColumn );

            var services = dataJson[lifecycle].instances.instanceCount;
            var projectUrl = dataJson[lifecycle].projectUrl;
            console.log( services )
            for ( var i = 0; i < services.length; i++ ) {
                var serviceSummary = services[i];
                var serviceName = serviceSummary.serviceName;
                var serviceClass = serviceName + "Row";
                var $serviceRow = $( "." + serviceClass );
                if ( $serviceRow.length == 0 ) {
                    $serviceRow = jQuery( '<tr/>', {class: serviceClass} );
                    $tableBody.append( $serviceRow );
                    var $nameColumn = jQuery( '<td/>', {text: serviceName} );
                    $serviceRow.append( $nameColumn );
                }

                // add placeholders for services not found in previous lifecycles
                for ( var numMissing = $serviceRow.children().length; numMissing < numLifecycles; numMissing++ ) {

                    var $countColumn = jQuery( '<td/>', {text: 0, class: "num"} );
                    $serviceRow.append( $countColumn );
                }

                var $projectLink = serviceSummary.count;
                if ( projectUrl != null ) {
                    $projectLink = jQuery( '<a/>', {
                        href: projectUrl + "/csap-admin/find-service/" + projName + "/" + serviceName,
                        title: "Click to open service portal",
                        text: serviceSummary.count,
                        target: "_blank",
                        class: "simple"
                    } );
                }


                var $countColumn = jQuery( '<td/>', {html: $projectLink, class: "num"} );
                $serviceRow.append( $countColumn );

            }

            var $sumColumn = jQuery( '<td/>', {'data-math': "col-sum", class: "num"} );
            $tableFootRow.append( $sumColumn );
            //updateTable(instances,"instanceDevTable","instanceDevBody",lifecycle);

        }


        // Add row total
        $tableHeadRow.append( jQuery( '<th/>', {text: "All Lifecycles"} ) );
        $( "tr", $tableBody ).each( function ( index ) {

            $serviceRow = $( this );
            for ( var numMissing = $serviceRow.children().length - 1; numMissing < numLifecycles; numMissing++ ) {

                var $countColumn = jQuery( '<td/>', {text: 0, class: "num"} );
                $serviceRow.append( $countColumn );
            }
            $serviceRow.append(
                    jQuery( '<td/>', {'data-math': "row-sum", class: "num"} )
                    );
        } );
        var $sumColumn = jQuery( '<td/>', {'data-math': "col-sum", class: "num"} );
        $tableFootRow.append( $sumColumn );

        $table.tablesorter( {
            sortList: [[0, 0]],
            theme: 'csapSummary',
            widgets: ['math'],
            widgetOptions: {
                math_mask: '#,###,##0.',
                math_data: 'math'
            }
        } );



    }

    function getParameterByName( name ) {
        name = name.replace( /[\[]/, "\\\[" ).replace( /[\]]/, "\\\]" );
        var regexS = "[\\?&]" + name + "=([^&#]*)", regex = new RegExp( regexS ), results = regex
                .exec( window.location.href );
        if ( results == null ) {
            return "";
        } else {
            return decodeURIComponent( results[1].replace( /\+/g, " " ) );
        }
    }
}
;
