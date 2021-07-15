

$(document).ready(function() {
	var demo = new Analytics();
	demo.appInit();
});


 

function Analytics() {
	var colorMap = new Object();

	var $adoptionBody = $("#csapAdoptionTable tbody");
	colorMap['total'] = '#00BFFF';
	this.appInit = function() {
		console.log("Init");

		$("#tabs").tabs({
			activate: function(event, ui) {
				copyShowGraphSelection(ui.newTab.index());
				drawAdoptionTrends();
			}
		});

		$("#csapAdoptionTable").tablesorter({
			sortList: [[1, 1]],
			theme: 'csapSummary',
			widgets: ['math', 'columnSelector'],
			widgetOptions: {
				columnSelector_container: $('#csapAdoptionColumn'),
				columnSelector_mediaquery: false,
				columnSelector_columns: {
					0: 'disable',
					2: false,
					3: false
				},
				columnSelector_saveColumns: false,
				math_mask: '##0',
				math_data: 'math',
				columns_tfoot: false,
				math_complete: tableMathFormat
			},
			headers: {
				// first column
				0: {
					sorter: false
				}
			}

		});

		$("#csapActivityTable").tablesorter({
			sortList: [[1, 1]],
			theme: 'csapSummary',
			widgets: ['math', 'columnSelector'],
			widgetOptions: {
				columnSelector_container: $('#csapActivityColumn'),
				columnSelector_mediaquery: false,
				columnSelector_columns: {
					0: 'disable',
					2: false,
					3: false
				},
				columnSelector_saveColumns: false,
				math_mask: '##0',
				math_data: 'math',
				columns_tfoot: false,
				math_complete: tableMathFormat
			},
			headers: {
				// first column
				0: {
					sorter: false
				}
			}

		});

		$('#numDaysSelect').change(function() {
			loadGlobalMetrics();
		});


		$('#trendingDaysSelect').change(function() {
			loadGraphs();
		});


		$('#showAdmin').change(function() {
			loadCsapAnalyticsSuccess(metricsData);
		});

		$('#filterButton').click(function() {
			$('.filters').toggle();
		});

		$(".graph").hover(function() {
			if ($('#csapAdoptionTable input:checkbox:checked.showGraphs').length > 6) {
				$(".legend", $(this)).show();
			}
		}, function() {
			if ($('#csapAdoptionTable input:checkbox:checked.showGraphs').length > 6) {
				$(".legend", $(this)).hide();
			}
		});

		$.when(loadDisplayNames()).then(loadGlobalMetrics);
	};

	var displayNameData;
	function loadDisplayNames() {
		var r = $.Deferred();
		$.getJSON("api/report/displayNames", {
		})
			.done(function(loadJson) {
				displayNameData = loadJson;
			});
		setTimeout(function() {
			console.log('loading display names  done');
			r.resolve();
		}, 500);
		return r;
	}

	var displayNameInfo;

	function loadHealthData() {
		$.getJSON("api/report/health", {
		})
			.done(function(loadJson) {
				loadHealthDataSuccess(loadJson);
			});
	}

	function loadHealthDataSuccess(dataJson) {
		$("#csapActivityTable tbody tr").each(function(i) {
			//var $this = $(this);
			var my_td = $(this).children("td");
			var second_col = my_td.eq(2).text();
			var healthInfo = dataJson[second_col.trim()];
			var totalVm;
			var healthyVm;
			if (healthInfo) {
				totalVm = healthInfo.totalvm;
				healthyVm = healthInfo.healthyvm;
			}
			addHealthInfo(healthyVm, totalVm, my_td, second_col.trim());

		});
	}

	this.loadGlobalMetrics = loadGlobalMetrics;
	function loadGlobalMetrics() {
		$('body').css('cursor', 'wait');

		var loadingImage = '<img title="Querying Events for counts" id="loadCountImage"  src="' + baseUrl + 'images/animated/loadLarge.gif"/>';
		$(".analyticsLineGraph").html(loadingImage);

		var tableLoad = '<tr><td colspan="99"><div class="loadingLargePanel" id="loadMessage" >Data is being loaded - time taken is proportional to time period selected.</div></td></tr>'
		$adoptionBody.html(tableLoad);

		$.when(loadCsapAdoption()).then(loadGraphs).then(loadHealthData);
	}
	var metricsData;
	this.loadCsapAdoption = loadCsapAdoption;
	function loadCsapAdoption() {
		var r = $.Deferred();
		$.getJSON(baseUrl + "api/report/adoption/current", {
			"numDays": $('#numDaysSelect').val()
		})
			.done(function(loadJson) {
				metricsData = loadJson;
				loadCsapAnalyticsSuccess(loadJson);
			})

			.fail(function(jqXHR, textStatus, errorThrown) {

				handleConnectionError("Getting analytics", errorThrown);
			});
		setTimeout(function() {
			console.log('loading csap adaption done');
			r.resolve();
		}, 500);
		return r;
	}
	var graphsData;
	this.loadGraphs = loadGraphs;
	function loadGraphs() {

		var r = $.Deferred();
		$.getJSON("api/report/adoption/trends", {
			"projectName": $('#projectSelect').val(),
			numDays: $('#trendingDaysSelect').val()

		}).done(function(loadJson) {
			graphsData = loadJson;
			var grapsToPlot = ['total'];
			buildProjectDataThenRender(loadJson, grapsToPlot);
			drawAdoptionTrends();
		});


		setTimeout(function() {
			console.log('loading graphs done');
			r.resolve();
		}, 500);
		return r;
	}

	this.loadCsapAnalyticsSuccess = loadCsapAnalyticsSuccess;
	function loadCsapAnalyticsSuccess(dataJson) {
		$("#csapAdoptionTable tbody").empty();
		$("#csapActivityTable tbody").empty();
		constructColorMap(dataJson);
		var isShowAdmin = false;
		if ($('#showAdmin').prop('checked')) {
			isShowAdmin = true;
		}
		for (var i = 0; i < dataJson.length; i++) {
			if ('total' == dataJson[i]._id)
				continue;
			var localPackName = dataJson[i]._id;
			var isHidden = false;
			var hrefUrl = "";
			var businessProgramName = "";
			if (displayNameData[dataJson[i]._id] && displayNameData[dataJson[i]._id].displayName) {
				businessProgramName = displayNameData[dataJson[i]._id].displayName;
			} else {
				businessProgramName = dataJson[i]._id;
			}

			var adoptionMetricsTrContent = '<td> ' + constructGraphsColumn(dataJson, i, "_adoptionGraphCheck", true) + '</td>' +
				'<td  class="csapPack"> ' + getBusinessProgramUrl(dataJson, businessProgramName, i) + '</td>' +
				'<td>' + dataJson[i]._id + '</td>' +
				'<td>' + getDeploymentName(dataJson[i].deploymentName) + '</td>' +
				'<td> ' + precise_round(dataJson[i].activeUsers, 0) + '</td>' +
				'<td> ' + precise_round(dataJson[i].vms, 0) + '</td>' +
				'<td> ' + precise_round(dataJson[i].cpuCount, 0) + '</td>' +
				'<td> ' + precise_round(dataJson[i].serviceCount, 0) + '</td>' +
				'<td> ' + precise_round(dataJson[i].instanceCount, 0) + '</td>' +
				'<td>' + getCsapVersion(dataJson[i].csapVersion) + '</td>'
				;
			var activityMetricsTrContent = '<td> ' + constructGraphsColumn(dataJson, i, "_activityGraphCheck", false) + '</td>' +
				'<td class="csapPack"> ' + businessProgramName + '</td>' +
				'<td>' + dataJson[i]._id + '</td>' +
				'<td>' + getDeploymentName(dataJson[i].deploymentName) + '</td>' +
				'<td> ' + precise_round(dataJson[i].activeUsers, 0) + '</td>' +
				'<td> ' + precise_round(dataJson[i].totalLoad, 0) + '</td>' +
				'<td> ' + averageLoad(dataJson[i].totalLoad, dataJson[i].numSamples) + '</td>' +
				'<td> ' + precise_round(dataJson[i].cpuCount, 0) + '</td>' +
				'<td> </td>'
				;
			var adoptionMetricsTr = $('<tr />', {
				'class': "",
				html: adoptionMetricsTrContent
			});
			var activityMetricsTr = $('<tr />', {
				'class': "",
				html: activityMetricsTrContent
			});
			$('#csapAdoptionTable tbody').append(adoptionMetricsTr);
			$('#csapActivityTable tbody').append(activityMetricsTr);
		}
		var adoptionTableId = "#csapAdoptionTable";
		$.tablesorter.computeColumnIndex($("tbody tr", adoptionTableId));
		$(adoptionTableId).trigger("tablesorter-initialized");
		var activityTableId = "#csapActivityTable";
		$.tablesorter.computeColumnIndex($("tbody tr", activityTableId));
		$(activityTableId).trigger("tablesorter-initialized");
		registerOnChange();

		$('body').css('cursor', 'default');
	}

	function getBusinessProgramUrl(dataJson, displayName, counter) {
		var businessProgramName = dataJson[counter]._id;
		var appId = dataJson[counter].appId;
		if (null != appId) {
			var href = '<a class="simple" target="_blank" href="' + analyticsPortal + '?life=dev&project=' + businessProgramName + '&appId=' + appId + '">' + displayName + '</a>';
			//console.log(href);
			return href;
		} else {
			return displayName;
		}
	}

	function averageLoad(totalLoad, numSamples) {
		if (0 != totalLoad && 0 != numSamples) {
			return precise_round((totalLoad / numSamples), 2);
		} else {
			return 0;
		}

	}

	function getDeploymentName(deploymentName) {
		if (null == deploymentName) {
			return "NA";
		} else {
			return deploymentName;
		}
	}

	function getCsapVersion(csapVersion) {
		if (null == csapVersion) {
			return "NA";
		} else {
			return csapVersion;
		}
	}
	this.constructGraphsColumn = constructGraphsColumn;
	function constructGraphsColumn(dataJson, counter, idSuffix, checkTheCheckBox) {
		var projectParameter = getParameterByName("projectName");
		var selectGraph = "";
		if (projectParameter === dataJson[counter]._id && checkTheCheckBox) {
			selectGraph = "checked";
		}
		var graphsColumn = '<input type="checkbox" class="showGraphs" id="' + counter + idSuffix + '"value="' + dataJson[counter]._id + '" ' + selectGraph;
		graphsColumn += ' /> ';
		return graphsColumn;
	}

	function constructColorMap(dataJson) {
		var keys = [];
		for (var i = 0; i < dataJson.length; i++) {
			keys.push(dataJson[i]._id);
		}
		keys.sort();
		/*var colors = ['#B0171F','#32CD32',
		 '#FFE7BA','#87CEFF','#FFFF00',
		 '#EEB4B4','#00FF7F','#DAA520',
		 '#FFF68F','#FF83FA','#FF8C00',
		 '#4876FF','#00868B','#FF4500',
		 '#00EE76','#71C671','#1A1A1A'
		 ];*/

		var colors = ["#00E5EE", "#FAA43A", "#60BD68",
			"#F17CB0", "#B2912F", "#B276B2",
			"#DECF3F", "#F15854", "#4D4D4D",
			'#87CEFF', '#FFFF00',
			'#EEB4B4', '#00FF7F', '#DAA520',
			'#FFF68F', '#FF83FA', '#FF8C00',
			'#4876FF', '#00868B', '#FF4500',
			'#00EE76', '#71C671', '#1A1A1A'
		];

		colorMap = new Object();
		colorMap['total'] = '#00BFFF';
		for (var i = 0; i < keys.length; i++) {
			if ('total' == keys[i])
				continue;
			colorMap[keys[i]] = colors[i];
		}
		console.log(colorMap);

	}
	this.registerOnChange = registerOnChange;
	function registerOnChange() {
		$('.showGraphs').change(function() {
			drawAdoptionTrends();
		});
	}

	function addHealthInfo(healthy, total, col, projName) {
		if (total) {
			var content = "";
			if (healthy == total) {
				content = '<img src="images/correct.gif">';
			} else {
				content = '<img src="images/warning.png">';
			}
			content = content + " (" + healthy + "/" + total + " )";
			var hrefContent = content;
			if (healthy != total) {
				hrefContent = '<a class="simple" target="_blank" href="' + 'health?projName=' + projName + '" >' + content + '</a>' + '</td>';
			}
			//col.eq(8).append("");
			col.eq(8).append(hrefContent);
		} else {
			col.eq(8).append('<img src="images/note.png" title="No Data">');
		}
	}

	this.constructHealthColumn = constructHealthColumn;
	function constructHealthColumn(dataJson, counter) {
		var healthyColumn = '<td style="font-size: 1.0em"> ';
		var healthyImage = "";
		var healthyVmCount = "";

		if (null == dataJson[counter].isHealthy) {
			healthyImage = '<img src="images/note.png" title="No Data">';
		} else {
			if (dataJson[counter].isHealthy) {
				healthyImage = '<img src="images/correct.gif">';
			} else {
				healthyImage = '<img src="images/warning.png">';
			}
			healthyVmCount = " (" + dataJson[counter].healthyVm + "/" + dataJson[counter].totalVm + ")";
		}
		if (null != dataJson[counter].isHealthy) {

			healthyColumn += '<a class="simple" target="_blank" href="' + 'health?businessProgram=' + dataJson[counter].packageName
				+ '&projectName=' + dataJson[counter].projectName
				+ '&lifecycle=' + dataJson[counter].lifeCycle
				+ '" >' + healthyImage + healthyVmCount + '</a>' + '</td>';
		} else {
			healthyColumn += healthyImage + '</td>';
		}
		return healthyColumn;
	}

	this.precise_round = precise_round;
	function precise_round(value, decPlaces) {
		var val = value * Math.pow(10, decPlaces);
		var fraction = (Math.round((val - parseInt(val)) * 10) / 10);
		if (fraction == -0.5)
			fraction = -0.6;
		val = Math.round(parseInt(val) + fraction) / Math.pow(10, decPlaces);
		return val;
	}

	this.drawAdoptionTrends = drawAdoptionTrends;
	function drawAdoptionTrends() {

		var selectedProjects = $('input:checkbox:checked.showGraphs').map(function() {
			return this.value;
		}).get();

		var ids = $('input:checkbox:checked.showGraphs').map(function() {
			return this.id;
		}).get();

		// Always include the total adoption settings
		selectedProjects.push('total');

		buildAdoptionTrendGraphs(selectedProjects);
	}

	function getGraphDataForProject(key, keys) {
		console.log("getGraphDataForProject() - ", " key: " + key, " keys:" + keys);
		var showPackage = true;
		if ($('#showPackages').prop('checked')) {
			showPackage = false;
		}
		$.getJSON("../rest/analytics/graphByProjectFilter", {
			"showPackage": showPackage,
			"projectName": key
		})
			.done(function(loadJson) {
				graphsData.usersGraphData[key] = loadJson.usersGraphData[key];
				graphsData.userActivityGraphData[key] = loadJson.userActivityGraphData[key];
				graphsData.servicesGraphData[key] = loadJson.servicesGraphData[key];
				graphsData.instancesGraphData[key] = loadJson.instancesGraphData[key];
				graphsData.resource_30_totalGraphData[key] = loadJson.resource_30_totalGraphData[key];
				graphsData.hostsGraphData[key] = loadJson.hostsGraphData[key];
				graphsData.cpusGraphData[key] = loadJson.cpusGraphData[key];
				buildProjectDataThenRender(graphsData, keys);

			});
	}

	function buildAdoptionTrendGraphs(projectsToDisplay) {
		console.log("buildProjectTrendGraphs() - ", " projectsToDisplay:" + projectsToDisplay);
		var additionalDataExists = true;
		for (var i = 0; i < projectsToDisplay.length; i++) {
			var key = projectsToDisplay[i];
			if (!graphsData.usersGraphData[key]) {
				getGraphDataForProject(key, projectsToDisplay);
				additionalDataExists = false;
			}
		}
		if (additionalDataExists) {
			buildProjectDataThenRender(graphsData, projectsToDisplay);
		}
	}
	function getDisplayLabel(key) {
		//console.log("------->>>>>>"+key);
		var displayName = "";
		if (displayNameData[key] && displayNameData[key].displayName) {
			displayName = displayNameData[key].displayName;
		} else {
			displayName = key;
		}
		return displayName;
	}

	function buildProjectDataThenRender(dataJson, selectedProjects) {

		console.log("buildProjectDataThenRender() - ", " selectedProjects:" + selectedProjects);
		var graphWidth = $("section.graph:visible").first().outerWidth(true) - 100;

		//console.log( "Setting widths: " + graphWidth );
		$(".analyticsLineGraph").css("width", graphWidth);

		var usersData = [];
		var userActivityData = [];
		var servicesData = [];
		var instancesData = [];
		var resource_30Data = [];
		var hostsData = [];
		var cpusData = [];

		for (var i = 0; i < selectedProjects.length; i++) {

			var key = selectedProjects[i];
			var displayLabel = getDisplayLabel(key);
			console.log("Key " + key);
			console.log("color key " + colorMap[key]);

			var userGraphElement = new Object();
			userGraphElement.label = displayLabel;
			userGraphElement.data = dataJson.usersGraphData[key];
			userGraphElement.color = colorMap[key];
			usersData.push(userGraphElement);

			var userActivityGraphElement = new Object();
			userActivityGraphElement.label = displayLabel;
			userActivityGraphElement.data = dataJson.userActivityGraphData[key];
			userActivityGraphElement.color = colorMap[key];
			userActivityData.push(userActivityGraphElement);

			var servicesGraphElement = new Object();
			servicesGraphElement.label = displayLabel;
			servicesGraphElement.data = dataJson.servicesGraphData[key];
			servicesGraphElement.color = colorMap[key];
			servicesData.push(servicesGraphElement);

			var instancesGraphElement = new Object();
			instancesGraphElement.label = displayLabel;
			instancesGraphElement.data = dataJson.instancesGraphData[key];
			instancesGraphElement.color = colorMap[key];
			instancesData.push(instancesGraphElement);

			var resource_30GraphElement = new Object();
			resource_30GraphElement.label = displayLabel;
			resource_30GraphElement.data = dataJson.resource_30_totalGraphData[key];
			resource_30GraphElement.color = colorMap[key];
			resource_30Data.push(resource_30GraphElement);

			var hostsGraphElement = new Object();
			hostsGraphElement.label = displayLabel;
			hostsGraphElement.data = dataJson.hostsGraphData[key];
			hostsGraphElement.color = colorMap[key];
			hostsData.push(hostsGraphElement);

			var cpusGraphElement = new Object();
			cpusGraphElement.label = displayLabel;
			cpusGraphElement.data = dataJson.cpusGraphData[key];
			cpusGraphElement.color = colorMap[key];
			cpusData.push(cpusGraphElement);

		}
		plotLineGraph("usersGraph", usersData, "Engineers", true);
		plotLineGraph("userActivityGraph", userActivityData, "Engineers Activity");
		plotLineGraph("servicesGraph", servicesData, "Services");
		plotLineGraph("instancesGraph", instancesData, "Instances");
		plotLineGraph("resource_30Graph", resource_30Data, "Load");
		plotLineGraph("hostsGraph", hostsData, "Hosts");
		plotLineGraph("cpusGraph", cpusData, "Cpus");

	}



	function plotLineGraph(divId, data, yaxisLabel, isBuildLegend) {

		var fillGraphs = true;
		console.log("plotLineGraph()  " + yaxisLabel, " num lines: " + data.length);

		if (data.length > 1) {
			fillGraphs = false;
		}
		
		let myLegend ;
		if ( isBuildLegend ) {
			
			let $legend = $("#adoption-legend");
	
			myLegend = {
				show: true,
				container: $legend[0]
				//container: $( "div.legend", $graphContainer.parent() )
			}
			
			
			$("<div id='tooltip'></div>").css({
				position: "absolute",
				display: "none",
				border: "1px solid #fdd",
				padding: "2px",
				"background-color": "#fee",
				opacity: 0.80
			}).appendTo("body");
		
		}
		
		let $currentGraph = $("#" + divId) ;

		var currentGraph = $.plot( $currentGraph , data, {
			
			xaxis: {
				mode: "time", timeformat: "%m/%d", minTickSize: [1, "day"]
			},
			
			series: {
				lines: { 
					show: true,
					fill: fillGraphs 
				},
				points: {
					radius: 2,
					show: false,
					fill: true
				},
			},
			
			grid: {
				hoverable: true,
				borderWidth: 1
			},
			
			legend: myLegend, 
			
			selection: {
				mode: "x"
			}
		});

		//console.log('before bind....'+divId);
		$currentGraph.parent().bind("plotselected", function(event, ranges) {
			console.log('In bind ');
			currentGraph.getOptions().xaxes[0].min = ranges.xaxis.from;
			currentGraph.getOptions().xaxes[0].max = ranges.xaxis.to;
			currentGraph.setupGrid();
			currentGraph.draw();
			currentGraph.clearSelection();
		});
		

		$currentGraph.bind("plothover", function (event, pos, item) {

			if (!pos.x || !pos.y) {
				return;
			}

				var str = pos.y.toFixed(2) ;
				$("#hoverdata").text(str);


				if (item) {
					var x = item.datapoint[0].toFixed(2),
						y = item.datapoint[1].toFixed(2);

					$("#tooltip").html(item.series.label + " :  " + y)
						.css({top: item.pageY-35, left: item.pageX+5})
						.fadeIn(200);
						
				} else {
					$("#tooltip").hide();
				}
			
		});

		$("#placeholder").bind("plothovercleanup", function (event, pos, item) {
				$("#tooltip").hide();
		});
	}

	function copyShowGraphSelection(tabIndex) {
		if (tabIndex == 1) {
			$('#csapAdoptionTable').find('tr').each(function() {
				var row = $(this);
				if (row.find('input[type="checkbox"]').is(':checked')) {
					var checkBoxId = row.find('input[type="checkbox"]').attr('id');
					if (checkBoxId == 'adoptionTotalCheck') {
						$('#activityTotalCheck').prop('checked', true);
					} else {
						var checkBoxCounter = checkBoxId.substring(0, checkBoxId.indexOf("_"));
						var activityCheckBoxId = checkBoxCounter + '_activityGraphCheck';
						console.log("activityCheckBoxId" + activityCheckBoxId);
						$('#' + activityCheckBoxId).prop('checked', true);
					}
					$('#' + checkBoxId).prop('checked', false);
				}
			});
		}
		if (tabIndex == 0) {
			$('#csapActivityTable').find('tr').each(function() {
				var row = $(this);
				if (row.find('input[type="checkbox"]').is(':checked')) {
					var checkBoxId = row.find('input[type="checkbox"]').attr('id');
					if (checkBoxId == 'activityTotalCheck') {
						$('#adoptionTotalCheck').prop('checked', true)
					} else {
						var checkBoxCounter = checkBoxId.substring(0, checkBoxId.indexOf("_"));
						var adoptionCheckBoxId = checkBoxCounter + '_adoptionGraphCheck';

						$('#' + adoptionCheckBoxId).prop('checked', true);
					}
					$('#' + checkBoxId).prop('checked', false);
				}
			});
		}
	}

	function getCheckBoxIdFromRow(tableRow) {
		tableRow.find('input[type="checkbox"]').each(function() {
			if (this.checked)
				alertify.alert("id" + this.id);
		});
		return "";
	}

	function getParameterByName(name) {
		name = name.replace(/[\[]/, "\\\[").replace(/[\]]/, "\\\]");
		var regexS = "[\\?&]" + name + "=([^&#]*)",
			regex = new RegExp(regexS),
			results = regex.exec(window.location.href);
		if (results == null) {
			return "";
		} else {
			return decodeURIComponent(results[1].replace(/\+/g, " "));
		}
	}

	var tableMathFormat = function($cell, wo, result, value, arry) {
		var txt = '<span class="align-decimal">' + result + '</span>';
		if ($cell.attr('data-prefix') != null) {
			txt = $cell.attr('data-prefix') + txt;
		}
		if ($cell.attr('data-suffix') != null) {
			txt += $cell.attr('data-suffix');
		}
		return txt;
	}
}