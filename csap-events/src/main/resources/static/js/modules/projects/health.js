$( document ).ready( function () {
    var health = new Health();
    health.appInit();

} );
function Health() {
    this.appInit = function () {
        console.log( "Init in health..." );

    };
    $.when( loadHealthInfo() ).then( addTableSorter );

    function loadHealthInfo() {
        var r = $.Deferred();
        var projName = getParameterByName( "projName" );
        $.getJSON( "api/report/healthMessage", {
            "projectName": projName
        } ).done( function ( loadJson ) {
            healthMessageSuccess( loadJson );
        } );
        setTimeout( function () {
            console.log( 'loading health info  done' );
            r.resolve();
        }, 500 );
        return r;
    }
    function addTableSorter() {
        $( "#healthTable" ).tablesorter( {
            sortList: [[0, 0]]
        } );
    }

    function healthMessageSuccess( dataJson ) {
        for ( var i = 0; i < dataJson.length; i++ ) {
            var hostName = dataJson[i].host;

            var data = dataJson[i].data;
            var errors = data.errors;
            var errorList = errors[hostName];
            var healthContent = '<td style="font-size: 1.0em" > ' + hostName + '</td>'
                    + '<td style="font-size: 1.0em" > ' + dataJson[i].lifecycle + '</td>'
                    + '<td style="font-size: 1.0em; " class="errorMessage" > ' + getErrorMessage( errorList ) + '</td>';
            var healthContentTr = $( '<tr />', {
                'class': "",
                html: healthContent
            } );
            $( '#healthBody' ).append( healthContentTr );
        }
    }

    function getErrorMessage( errorList ) {
        var errorMessage = '';
        if ( errorList.length > 0 ) {
            var containerObject = jQuery( '<div/>' );
            var errorObject = jQuery( '<ol/>', {
                class: " ",
                title: "List of errors"
            } ).css( {
                "padding": "3px",
                "list-style-type": "decimal"
            } ).appendTo( containerObject );
            for ( var i = 0; i < errorList.length; i++ ) {
                //errorMessage = errorMessage +'\n' +errorList[i] + ' ';
                //errorMessage.append()
                jQuery( '<li/>', {
                    class: "",
                    text: errorList[i]
                } ).css( {
                    "padding": "2px"
                } ).appendTo( errorObject );
            }
            errorMessage = containerObject.html();
        }
        return errorMessage;
        /*
         if(dataJson[i].errors.states.processes != undefined){
         return dataJson[i].errors.states.processes.message;
         }
         if(dataJson[i].errors.states.memory != undefined){
         return dataJson[i].errors.states.memory.message;
         }
         */
        //return '';

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