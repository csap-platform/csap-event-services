define([], function () {

	 console.log("Host module loaded");
	 var hostData;
	 
	 $(document).ready(function () {
		  initialize();
	 });
	 return {
		 //
		  createHostTable: function (dataJson) {
			  createHostTable(dataJson)
		  }
	 }
	 /** @memberOf host */
	 function createHostTable(dataJson) {
		 hostData = dataJson;
		 $(".hostClass").show();
		  var hostTable = $("#hostTable");
		  $("#hostTable tbody").empty();
		  var hostBody = jQuery('<tbody/>', {});
		  hostTable.append(hostBody);
		  var filterVal = $("#filterCpu").val();
		  console.log("filterVal"+filterVal);
		  for (var i = 0; i < dataJson.data.length; i++) {
			    var totalCpu = dataJson.data[i].data.summary.totalUsrCpu + dataJson.data[i].data.summary.totalSysCpu;
			    
			    //var coresUsed = ((dataJson.data[i].data.summary.totalUsrCpu + dataJson.data[i].data.summary.totalSysCpu ) / dataJson.data[i].data.summary.numberOfSamples) * dataJson.data[i].data.summary.cpuCountAvg;
			    var percentCoresUsed = ((dataJson.data[i].data.summary.totalUsrCpu + dataJson.data[i].data.summary.totalSysCpu ) / dataJson.data[i].data.summary.numberOfSamples) / 100 ;
			    var vmCoresUsed = percentCoresUsed *  dataJson.data[i].data.summary.cpuCountAvg;
			     
				if (vmCoresUsed < filterVal)
					 continue;
				var hostRow = jQuery('<tr/>', {});
				var hostName = dataJson.data[i].host ;
//				if (dataJson.data[i].host.indexOf("yourcompany.com") > 0) {
//					 hostName = dataJson.data[i].host;
//				} else {
//					 hostName = dataJson.data[i].host + ".yourcompany.com";
//				}
				var hostHref = "<a target='_blank' class='simple' href=http://" + hostName + ":8011/os/HostDashboard?offset=420&" + ">" + dataJson.data[i].host + "</a>";
				hostRow.append(jQuery('<td/>', {
					 html: hostHref
				}));
				hostRow.append(jQuery('<td/>', {
					 text: dataJson.data[i].data.summary.cpuCountAvg
				}));
				
				hostRow.append(jQuery('<td/>', {
					 text: precise_round(vmCoresUsed , 2)
				}));
				hostRow.append(jQuery('<td/>', {
					 text: precise_round((dataJson.data[i].data.summary.totalUsrCpu / dataJson.data[i].data.summary.numberOfSamples),2)
				}));
				hostRow.append(jQuery('<td/>', {
					 text: precise_round((dataJson.data[i].data.summary.totalSysCpu / dataJson.data[i].data.summary.numberOfSamples),2)
				}));
				hostRow.append(jQuery('<td/>', {
					 text: precise_round(dataJson.data[i].data.summary.alertsCount / dataJson.data[i].data.summary.numberOfSamples , 0 )
				}));
				hostRow.append(jQuery('<td/>', {
					 text: precise_round(dataJson.data[i].data.summary.totalLoad/dataJson.data[i].data.summary.numberOfSamples,2)
				}));
				hostRow.append(jQuery('<td/>', {
					 text: precise_round(dataJson.data[i].data.summary.totalIo / dataJson.data[i].data.summary.numberOfSamples ,2)
				}));

				hostBody.append(hostRow);
		  }
		  $("#hostTable").tablesorter({
				sortList: [[2, 1]]
				, theme: 'csapSummary'
		  });
		  var hostTableId = "#hostTable";
		  $.tablesorter.computeColumnIndex($("tbody tr", hostTableId));
		  $(hostTableId).trigger("updateAll");
		  $('body').css('cursor', 'default');

	 }
	 function initialize(){
		  $("#filterCpu").change(function () {
			  createHostTable(hostData);
		  });
	 }
});
