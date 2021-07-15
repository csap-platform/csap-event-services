$(document).ready(function() {

	console.log(`\n\n **** loaded  ***\n\n`);
	let summary = new Summary();
	summary.appInit();

});

function Summary() {

	let $minimumHosts = $("#minimumHosts");

	this.appInit = function() {
		console.log("Init in summary");
	};



	$('#metrics-collection-button').click(function() {
		document.location.href = baseUrl + 'trends';
	});

	$minimumHosts.change(function() {
		getCsapAdoptionReport();
	});

	$.when(loadDisplayNames()).then(getCsapAdoptionReport).then(
		buildAdoptionTotals);

	let metricsDataMap = null;

	function getMetricsSamplesForBaselines() {

		console.log("Getting reports to build size estimates");
		metricsDataMap = new Object();

		$.getJSON(api + "/report/reportCounts", {}).done(function(dataJson) {

			metricsDataMap['hostServiceDataJson'] = dataJson;

		});

		let life = globalLife;

		let params = {
			numberOfDays: "1",
			appId: "sensusDevops",
			life: life,
			dateOffSet: "1",
			padLatest: false
		}

		let hostUrl = baseUrl + "api/metrics/some-host-name";
		$.getJSON(hostUrl + "/host_30", params).done(function(dataJson) {
			let totalHostLength = 0;
			for (let key in dataJson.data) {
				if (key === "timeStamp")
					continue;
				totalHostLength = totalHostLength + dataJson.data[key].length;
			}
			metricsDataMap['totalHostLength'] = totalHostLength;
		});

		$.getJSON(hostUrl + "/os-process_30", params)
			.done(
				function(dataJson) {
					let serviceReportLength = 0;
					for (let key in dataJson.data) {
						if (key === "timeStamp")
							continue;
						if (key.indexOf("csap-agent") > 1) {
							serviceReportLength = serviceReportLength
								+ dataJson.data[key].length;
						}
					}
					metricsDataMap['serviceReportLength'] = serviceReportLength;
				});

		$.getJSON(hostUrl + "/jmx_30", params).done(
			function(dataJson) {
				let jmxReportLength = 0;
				for (let key in dataJson.data) {
					if (key === "timeStamp")
						continue;
					if (key.indexOf("csap-agent") > 1) {
						jmxReportLength = jmxReportLength
							+ dataJson.data[key].length;
					}
				}
				metricsDataMap['jmxReportLength'] = jmxReportLength;
			});

	}

	function buildAdoptionTotals() {
		console.log("buildAdoptionTotals")

		getMetricsSamplesForBaselines();

		$(document).ajaxStop(
			function() {
				console.log(`buildAdoptionTotals: `, metricsDataMap)
				// console.log("metricsDataMap
				// &&&--->"+JSON.stringify(metricsDataMap));
				let toolTip = "Reports:     "
					+ metricsDataMap['hostServiceDataJson'].hostReportCount
					+ "\nPoints per day: "
					+ metricsDataMap['totalHostLength'];
				$("#hostDataPoints").attr("title", toolTip);

				$('#metrics-collection-button span').text(metricsDataMap['hostServiceDataJson'].daysAvailableForHostData + " days stored");

				console.log(`hosts:  ${metricsDataMap['totalHostLength']}  reportCount: ${metricsDataMap['hostServiceDataJson'].hostReportCount}`);

				let totalDataPoints = metricsDataMap['totalHostLength']
					* metricsDataMap['hostServiceDataJson'].hostReportCount
					/ 1000000000;
				$("#hostData").html(totalDataPoints.toFixed(3));

				let serviceToolTip = "Reports:     "
					+ metricsDataMap['hostServiceDataJson'].serviceReportCount
					+ "\nService points per day: "
					+ metricsDataMap['serviceReportLength']
					+ "\nJmx points per day: "
					+ metricsDataMap['jmxReportLength'];

				$("#serviceDataPoints").attr("title",
					serviceToolTip);
				// multiply by 2 to get service and jmx reports
				// total
				let avgServiceReport = totalInstanceCount
					/ (metricsDataMap['hostServiceDataJson'].serviceReportCountOneDay * 2);

				let divideBy = BILLIONS;

				console.log("metricsDataMap:", metricsDataMap);
				let totalServiceDataPoints = (metricsDataMap['serviceReportLength'] + metricsDataMap['jmxReportLength'])
					* metricsDataMap['hostServiceDataJson'].serviceReportCount
					* avgServiceReport / divideBy;
				$("#serviceData").html(
					totalServiceDataPoints.toFixed(3));
			});
	}
	let BILLIONS = 1000000000;
	let MILLIONS = 1000000;
	let THOUSANDS = 1000;

	let displayNameData;
	function loadDisplayNames() {
		let r = $.Deferred();
		$.getJSON(api + "/report/displayNames", {}).done(function(loadJson) {
			displayNameData = loadJson;
		});
		setTimeout(function() {
			console.log('loading display names  done');
			r.resolve();
		}, 500);
		return r;
	}

	function getCsapAdoptionReport() {

		let loadingImage = '<img title="Querying Events for counts" id="loadCountImage" width="14" src="'
			+ baseUrl + 'images/animated/loadSmall.gif"/>';
		$(".value").html(loadingImage);
		console.log("Loading Adoption Summary")
		$('body').css('cursor', 'wait');
		let params = {
			"days": $('#numDaysSelect').val()
		};
		$.getJSON(api + "/report/adoption/current", params)

			.done(function(loadJson) {
				processAdoptionData(loadJson);
			})

			.fail(function(jqXHR, textStatus, errorThrown) {

				handleConnectionError("Getting analytics", errorThrown);

			});
	}

	let totalInstanceCount = 0;
	function processAdoptionData(dataJson) {
		let totalBusinessPrograms = 0;
		let totalHosts = 0;
		let totalCpus = 0;
		let totalEngineers = 0;
		let totalServices = 0;
		let totalInstances = 0;
		let totalActivity = 0;
		let hostsPie = [];
		let cpuPie = [];
		let servicesPie = [];
		let serviceInstancesPie = [];
		let usersPie = [];
		let userActivityPie = [];
		let minimumHostFilter = $minimumHosts.val();
		console.log("vmFilter" + minimumHostFilter);
		let colorMap = constructColorMap(dataJson);

		let numFilteredHosts = 0
		for (let i = 0; i < dataJson.length; i++) {
			if (dataJson[i].vms < minimumHostFilter)
				continue;
			numFilteredHosts++;
		}

		if (numFilteredHosts == 0) {
			alertify.notify("Reseting minimum hosts to 1");
			$minimumHosts.val(1);
			minimumHostFilter = 1;
		}

		for (let i = 0; i < dataJson.length; i++) {
			let localPackName = dataJson[i]._id;
			let isHidden = false;

			if (displayNameData[dataJson[i]._id]
				&& displayNameData[dataJson[i]._id].hidden) {
				isHidden = displayNameData[dataJson[i]._id].hidden;
			}

			if ('total' == dataJson[i]._id)
				continue;
			totalInstanceCount += dataJson[i].instanceCount;
			if (dataJson[i].vms < minimumHostFilter || isHidden)
				continue;

			let key = dataJson[i]._id;
			totalHosts += dataJson[i].vms;
			totalCpus += dataJson[i].cpuCount;
			totalEngineers += dataJson[i].activeUsers;
			totalServices += dataJson[i].serviceCount;
			totalInstances += dataJson[i].instanceCount;
			totalActivity += dataJson[i].totalActivity;

			totalBusinessPrograms++;

			let displayLabel = "";
			if (displayNameData[dataJson[i]._id]
				&& displayNameData[dataJson[i]._id].displayName) {
				displayLabel = displayNameData[dataJson[i]._id].displayName;
			} else {
				displayLabel = dataJson[i]._id;
			}

			let hostPieElement = new Object();
			hostPieElement.label = displayLabel;
			hostPieElement.data = precise_round(dataJson[i].vms, 0);
			hostPieElement.color = colorMap[key];
			hostPieElement.url = baseUrl + 'trends?numberOfDays=1&projectName='
				+ dataJson[i]._id;
			hostsPie.push(hostPieElement);

			let cpuPieElement = new Object();
			cpuPieElement.label = displayLabel;
			cpuPieElement.data = precise_round(dataJson[i].cpuCount, 0);
			cpuPieElement.color = colorMap[key];
			cpuPieElement.url = baseUrl + 'trends?numberOfDays=1&projectName='
				+ dataJson[i]._id;
			cpuPie.push(cpuPieElement);

			let servicesPieElement = new Object();
			servicesPieElement.label = displayLabel;
			servicesPieElement.data = precise_round(dataJson[i].serviceCount, 0);
			servicesPieElement.color = colorMap[key];
			servicesPieElement.url = baseUrl
				+ 'trends?numberOfDays=1&projectName=' + dataJson[i]._id;
			servicesPie.push(servicesPieElement);

			let serviceInstancePieElement = new Object();
			serviceInstancePieElement.label = displayLabel;
			serviceInstancePieElement.data = precise_round(
				dataJson[i].instanceCount, 0);
			serviceInstancePieElement.color = colorMap[key];
			serviceInstancePieElement.url = 'services?projName='
				+ dataJson[i]._id + '&appId=' + dataJson[i].appId;
			// serviceInstancePieElement.url='analytics?projectName='+dataJson[i]._id;
			serviceInstancesPie.push(serviceInstancePieElement);

			let usersPieElement = new Object();
			usersPieElement.label = displayLabel;
			usersPieElement.data = precise_round(dataJson[i].activeUsers, 0);
			usersPieElement.color = colorMap[key];
			usersPieElement.url = baseUrl
				+ 'trends?numberOfDays=1&projectName=' + dataJson[i]._id;
			usersPie.push(usersPieElement);

			let userActivityPieElement = new Object();
			userActivityPieElement.label = displayLabel;
			userActivityPieElement.data = precise_round(
				dataJson[i].totalActivity, 0);
			userActivityPieElement.color = colorMap[key];
			userActivityPieElement.url = baseUrl
				+ 'trends?numberOfDays=1&projectName=' + dataJson[i]._id;
			userActivityPie.push(userActivityPieElement);

		}

		sortOnLabel(hostsPie);
		sortOnLabel(cpuPie);
		sortOnLabel(servicesPie);
		sortOnLabel(serviceInstancesPie);
		sortOnLabel(usersPie);
		sortOnLabel(userActivityPie);

		updateSummaryTable(totalBusinessPrograms, totalHosts, totalCpus,
			totalServices, totalInstances, totalEngineers, totalActivity);
		plotInteractivePie('hostsPieChart', hostsPie, false, true);
		plotInteractivePie('cpuPieChart', cpuPie, false);
		plotInteractivePie('servicesPieChart', servicesPie, false);
		plotInteractivePie('serviceInstancesPieChart', serviceInstancesPie, true);
		plotInteractivePie('userPieChart', usersPie, false);
		plotInteractivePie('userActivityPieChart', userActivityPie, false);

		$('body').css('cursor', 'default');

	}

	function constructColorMap(dataJson) {
		let keys = [];
		for (let i = 0; i < dataJson.length; i++) {
			let key = dataJson[i]._id;
			keys.push(key);
		}
		keys.sort();
		/*
		 * let colors = ['#B0171F','#32CD32', '#FFE7BA','#87CEFF','#FFFF00',
		 * '#EEB4B4','#00FF7F','#DAA520', '#FFF68F','#FF83FA','#FF8C00',
		 * '#4876FF','#00868B','#FF4500', '#00EE76','#71C671','#1A1A1A' ];
		 */
		let colors = ["#00E5EE", "#FAA43A", "#60BD68", "#F17CB0", "#B2912F",
			"#B276B2", "#DECF3F", "#F15854", "#4D4D4D", '#87CEFF',
			'#FFFF00', '#EEB4B4', '#00FF7F', '#DAA520', '#FFF68F',
			'#FF83FA', '#FF8C00', '#4876FF', '#00868B', '#FF4500',
			'#00EE76', '#71C671', '#1A1A1A'];
		let colorMap = new Object();
		colorMap['total'] = '#00BFFF';
		for (let i = 0; i < keys.length; i++) {
			if ('total' == keys[i])
				continue;
			colorMap[keys[i]] = colors[i];
		}
		console.log(colorMap);
		return colorMap;
	}

	function sortOnLabel(pieElement) {
		pieElement.sort(function(a, b) {
			// console.log(a);
			let x = a.label.toLowerCase();
			let y = b.label.toLowerCase();
			return x < y ? -1 : x > y ? 1 : 0;
		});
	}

	function plotInteractivePie(divId, data, newWindow, isBuildLegend ) {

		console.log("Building Graph: ", divId, data);
		let $graphContainer = $('#' + divId);
		
		let pieRadius = Math.round( $graphContainer.outerWidth( true ) / 2 ) ;
		console.log( `pieRadius: ${ pieRadius }` ) ;
		let myLegend ;
		if ( isBuildLegend ) {
			
			let $legend = $("#adoption-chart-legend");
	
			myLegend = {
				show: true,
				container: $legend[0]
				//container: $( "div.legend", $graphContainer.parent() )
			}
		}


		// console.log( "legend:  $legend.length " ) ;

		$.plot(

			$graphContainer,
			data,

			{
				series: {

					lines: {
						show: true,
						fill: true
					},

					pie: {
						show: true,
						margin: 0,
						borderwidth: 10,
						radius: pieRadius,
						label: {
							show: true,
							radius: 5 / 8,
							threshold: 0.05,
							// Added custom formatter here...
							formatter: function(label, series) {
								// alertify.log(data);
								// return(point.percent.toFixed(2) +
								// '%');
								// return (series.data[0][1]);
								return '<div style="font-size:16pt;text-align:center;padding:4px;color:black;">'
									+ series.data[0][1]
									+ '</div>';
							}

						}
					}
				},
				// https://github.com/flot/flot/blob/master/API.md#customizing-the-legend
				legend: myLegend,
				tooltip: true,
				grid: {
					hoverable: true,
					clickable: true
				}
			});

		$graphContainer.hover(function() {
			$(this).css('cursor', 'pointer');
		}, function() {
			$(this).css('cursor', 'normal');
		});

		$graphContainer.unbind("plotclick");
		$graphContainer.bind("plotclick", function(event, pos, obj) {
			if (newWindow) {
				window.open(data[obj.seriesIndex].url);
			} else {
				window.location.replace(data[obj.seriesIndex].url);
			}
		});

		$graphContainer.UseTooltip();
	}

	$.fn.UseTooltip = function() {
		$(this).bind("plothover", function(event, pos, item) {

			$("#plot-tool-tip").remove();

			if (item) {

				let x = item.datapoint[0];
				let y = item.datapoint[1];

				//                console.log(`label: ${item.series.label} x: ${x}  y: ${y}, pos.pageX: ${pos.pageX}  `, event, item)

				showTooltip(pos.pageX, pos.pageY,
					item.series.label + ":<span>" + y[0][1] + "</span>");
			}

		});
	};



	function showTooltip(x, y, contents) {
		$('<div id="plot-tool-tip">' + contents + '</div>').css({
			top: y - 50,
			left: x + 5
		}).appendTo("body");
	}

	function updateSummaryTable(totalProjects, totalHosts, totalCpus,
		totalServices, totalServiceInstances, totalUsers, totalUserActivity) {
		$("#projects").html(precise_round(totalProjects, 0));
		$("#hosts").html(
			precise_round(totalHosts, 0) + ' ('
			+ precise_round(totalCpus, 0) + ')');
		$("#services").html(precise_round(totalServices, 0));
		$("#instances").html(precise_round(totalServiceInstances, 0));
		$("#users").html(precise_round(totalUsers, 0));
		$("#userActivity").html(precise_round(totalUserActivity, 0));
	}

	function precise_round(value, decPlaces) {
		let val = value * Math.pow(10, decPlaces);
		let fraction = (Math.round((val - parseInt(val)) * 10) / 10);
		if (fraction == -0.5)
			fraction = -0.6;
		val = Math.round(parseInt(val) + fraction) / Math.pow(10, decPlaces);
		return val;
	}
}