const tableTitle = document.querySelector("#table-title");
const tableBody = document.querySelector("table#main-table tbody");
const workloadSelect = document.querySelector("select#workload-select");
const workloadTypeSelect = document.querySelector("select#workload-type");
const recordcountSelect = document.querySelector("select#recordcount-select");
const operationcountSelect = document.querySelector("select#operationcount-select");
const numNodesSelect = document.querySelector("select#node-num-select");

const metricSelect = document.querySelector("select#metric-select");
const chartSection = document.querySelector("div#chart-section");

const protocolSelect = document.querySelector("#select-protocol");
const consistencySelect = document.querySelector("#select-consistency");
const persistencySelect = document.querySelector("#select-persistency");
const operationSelect = document.querySelector("#select-operation");
const threadSelect = document.querySelector("#select-thread");
const latencySelect = document.querySelector("#select-latency");

const bgColors = {
    gray700: "#364153",
    gray950: "#030712",
};

const colors = {
    purple70: "#6929c4",
    cyan50: "#1192e8",
    teal70: "#005d5d",
    magenta70: "#9f1853",
    red50: "#fa4d56",
    red90: "#570408",
    green60: "#198038",
    blue80: "#002d9c",
    magenta50: "#ee538b",
    yellow50: "#b28600",
    teal50: "#009d9a",
    cyan90: "#012749",
    orange70: "#8a3800",
    purple50: "#a56eff",
};
const colorPalette = Object.values(colors);


const filter = {
    num_of_nodes: 0,
    workloadName: "",
    workloadType: "",
    protocol: "",
    consistency: "",
    persistency: "",
    operation: "",
    latency: "AverageLatency(us)",
    metric: "",
    threads: 0,
    operationCount: 0,
    recordCount: 0,
};

const renderedChart = {};

let prevChart = [];
Chart.defaults.color = bgColors.gray950;
//Chart.register(ChartDataLabels);

/* Helper Functions */
function loadSelectElement(elementPointer, values, fieldName, defaultValue = null) {
    // Clean content of element
    while (elementPointer.firstChild)
	elementPointer.removeChild(elementPointer.lastChild);

    // Load values
    values.forEach(value => {
	const option = document.createElement("option");
	option.setAttribute("value", value);
	option.textContent = value;
	elementPointer.appendChild(option);
    });

    // Set default value
    if (defaultValue !== null)
	filter[fieldName] = defaultValue;
    else
	filter[fieldName] = values[0];

    elementPointer.value = filter[fieldName];
}

function fetchData(filename) {
    fetch(filename).then(res => {
	if (!res.ok) throw new Error('Network response was not OK');
	console.error("error: ", res.json());
	return res.json();
    }).then(data => {
	const renamed = data.map(item => {
	    return { ...item, project: item.project.replaceAll(".", "/") }
	});

	const renamedData = JSON.stringify(renamed);
	return renamedData;
    }).catch(error => {
	console.error('Error fetching file:', error);
    });
}

function loadFilter(elementPointer, values, fieldName, defaultValue = null, ...funcs) {
    const radios = [];
    const labels = [];

    while (elementPointer.firstChild) elementPointer.removeChild(elementPointer.lastChild);
    values.forEach(item => {
	const label = document.createElement("label");
	label.classList.add("flex", "rounded-md", "px-2", "align-center", "border", "text-gray-950", "duration-100");
	label.setAttribute("data-val", item);

	if (filter[fieldName] === item) {
	    label.classList.add("bg-sky-500", "border-sky-500", "hover:bg-sky-600");
	} else {
	    label.classList.add("border-gray-700", "hover:bg-gray-300");
	}

	const input = document.createElement("input");
	input.classList.add("cursor-pointer", "sr-only");
	input.setAttribute("type", "radio");
	input.setAttribute("name", fieldName);
	input.setAttribute("value", item);

	const span = document.createElement("span");
	span.classList.add("text-xs");
	span.textContent = item;

	label.append(input, span);
	elementPointer.appendChild(label);
	radios.push(input);
	labels.push(label);
    });

    const data = sessionStorage.getItem("data");
    const json = JSON.parse(data);
    radios.forEach(radio => {
	radio.addEventListener("click", (e) => {
	    if (filter[fieldName] === e.target.value) return;

	    let prev = labels.find(item => item.getAttribute("data-val") === String(filter[fieldName])); 
	    prev.classList.add("border-gray-700", "hover:bg-gray-300");
	    prev.classList.remove("bg-sky-500", "border-sky-500", "hover:bg-sky-600");

	    filter[fieldName] = e.target.value;
	    let current = labels.find(item => item.getAttribute("data-val") === e.target.value); 
	    current.classList.add("bg-sky-500", "border-sky-500", "hover:bg-sky-600");
	    current.classList.remove("border-gray-700", "hover:bg-gray-300");

	    funcs.forEach(fn => {
		if (typeof fn === 'function') {
		    fn(json);
		} else {
		    console.log('Invalid function passed:', fn);
		}
	    });
	});
    });

    if (!filter[fieldName] || !values.includes(filter[fieldName])) {
	if (defaultValue)
	    filter[fieldName] = defaultValue;
	else
	    filter[fieldName] = values[0];
    }

    let initial = labels.find(item => item.getAttribute("data-val") === String(filter[fieldName])); 
    initial.classList.add("bg-sky-500", "border-sky-500", "hover:bg-sky-600");
    initial.classList.remove("border-gray-700", "hover:bg-gray-300");
}

function getFilteredOperations(json) {
    const operationSet = new Set();
    json.forEach(project => {
	project["protocols"].forEach(protocol => {
	    protocol["workloads"].forEach(workload => {
		if (workload["name"] !== filter["workloadName"]) return;

		workload["results"].forEach(result => {
		    Object.keys(result["result"]).forEach(key => { 
			if (key !== "OVERALL") operationSet.add(key)
		    });
		});
	    });
	});
    });

    return [...operationSet];
}

function getFilteredYcsbResult(json) {
    const filtered = [];

    json.forEach(project => {
	project["protocols"].forEach(protocol => {
	    if (filter["protocol"] !== "All" && protocol["name"] !== filter["protocol"])
		return;

	    if (filter["consistency"] !== "All" && protocol["consistency"] !== filter["consistency"])
		return;

	    if (filter["persistency"] !== "All" && protocol["persistency"] !== filter["persistency"])
		return;

	    protocol["workloads"].forEach(workload => {
		if (workload["name"] !== filter["workloadName"]
		    || workload["type"] !== filter["workloadType"]
		    || workload["operation_count"] !== filter["operationCount"]
		    || workload["record_count"] !== filter["recordCount"]
		) return;

		const projectItem = {
		    project: project["project"],
		    protocol: protocol["name"],
		    commit: protocol["commit"],
		}

		const foundResult = workload["results"].find(result => result["thread_count"] === Number(filter["threads"]));
		if (!foundResult) return;

		projectItem["result"] = foundResult["result"];
		filtered.push(projectItem);
	    });
	});
    });

    return filtered;
}
/* End of Helper Functions */

function loadSelectWorkload(json) {
    const workloads = [];
    json.forEach(project => {
	project["protocols"].forEach(protocol => {
	    protocol["workloads"].forEach(workload => {
		const alreadyExists = workloads.some(
		    w => w["name"] === workload["name"]
		    && w["type"] === workload["type"]
		    && w["operation_count"] === workload["operation_count"]
		    && w["record_count"] === workload["record_count"]
		    && w["request_distribution"] === workload["request_distribution"]
		    && w["read_proportion"] === workload["read_proportion"]
		    && w["update_proportion"] === workload["update_proportion"]
		    && w["read_modify_write_proportion"] === workload["read_modify_write_proportion"]
		    && w["insert_proportion"] === workload["insert_proportion"]
		    && w["num_of_nodes"] === workload["num_of_nodes"]
		    && w["seed"] === workload["seed"]
		)

		if (alreadyExists) return;

		workloads.push({
		    name: workload["name"],
		    type: workload["type"],
		    operation_count: workload["operation_count"],
		    record_count: workload["record_count"],
		    request_distribution: workload["request_distribution"],
		    read_proportion: workload["read_proportion"],
		    update_proportion: workload["update_proportion"],
		    read_modify_write_proportion: workload["read_modify_write_proportion"],
		    insert_proportion: workload["insert_proportion"],
		    num_of_nodes: workload["num_of_nodes"],
		    seed: workload["seed"],
		})
	    })
	})
    });

    const params = new URLSearchParams(window.location.search);
    const selectedWorkload = params.get("workload");
    const workloadNames = workloads.map(w => w["name"]);

    loadSelectElement(workloadSelect, workloadNames, "workloadName", selectedWorkload);
}

function loadSelectWorkloadType(json) {
    const workloadTypeSet = new Set();

    json.forEach(project => {
	project["protocols"].forEach(protocol => {
	    protocol["workloads"].forEach(workload => {
		if (workload["name"] === filter["workloadName"])
		    workloadTypeSet.add(workload["type"]);
	    });
	});
    });

    loadSelectElement(workloadTypeSelect, [...workloadTypeSet], "workloadType");
}

function loadSelectNumNodes(json) {
    const numNodesSet = new Set();

    json.forEach(project => {
	project["protocols"].forEach(protocol => {
	    protocol["workloads"].forEach(workload => {
		if (workload["name"] === filter["workloadName"])
		    numNodesSet.add(workload["num_of_nodes"]);
	    });
	});
    });

    loadSelectElement(numNodesSelect, [...numNodesSet], "num_of_nodes");
}

function loadSelectRecordCount(json) {
    const recordCountSet = new Set();

    json.forEach(project => {
	project["protocols"].forEach(protocol => {
	    protocol["workloads"].forEach(workload => {
		if (workload["name"] === filter["workloadName"])
		    recordCountSet.add(workload["record_count"]);
	    });
	});
    });

    loadSelectElement(recordcountSelect, [...recordCountSet], "recordCount");
}

function loadSelectOperationCount(json) {
    const operationCountSet = new Set();

    json.forEach(project => {
	project["protocols"].forEach(protocol => {
	    protocol["workloads"].forEach(workload => {
		if (workload["name"] === filter["workloadName"])
		    operationCountSet.add(workload["operation_count"]);
	    });
	});
    });

    loadSelectElement(operationcountSelect, [...operationCountSet], "operationCount");
}

function loadFilterProtocol(json) {
    const protocolSet = new Set();

    json.forEach(project => {
	project["protocols"].forEach(protocol => protocolSet.add(protocol["name"]));
    });

    loadFilter(protocolSelect, [...protocolSet, "All"], "protocol", "All", renderLatencyThroughputChart);
}

function loadFilterConsistency(json) {
    const consistencySet = new Set();

    json.forEach(project => {
	project["protocols"].forEach(protocol => consistencySet.add(protocol["consistency"]));
    });

    loadFilter(consistencySelect, [...consistencySet, "All"], "consistency", "All", renderLatencyThroughputChart);
}

function loadFilterPersistency(json) {
    const persistencySet = new Set();

    json.forEach(project => {
	project["protocols"].forEach(protocol => persistencySet.add(protocol["persistency"]));
    });

    loadFilter(persistencySelect, [...persistencySet, "All"], "persistency", "All", renderLatencyThroughputChart);
}

function renderProportionChart(workload) {
    const data = {
	insert: workload["insert_proportion"],
	read: workload["read_proportion"],
	update: workload["update_proportion"],
	readModifyWrite: workload["read_modify_write_proportion"],
    };
    if (renderedChart["proportion"]) renderedChart["proportion"].destroy();
    const canvas = document.querySelector("canvas#proportion-chart");

    while (canvas.firstChild) canvas.removeChild(canvas.lastChild);

    const labels = Object.entries(data)
	.filter(([key, value]) => value > 0)
	.map(([key, value]) => key);
    const chartData = Object.entries(data)
	.filter(([_, value]) => value > 0)
	.map(([_, value]) => value);

    const chart = new Chart(canvas, {
	type: "doughnut",
	options: {
	    responsive: true,
	    maintainAspectRatio: true,
	    plugins: {
		title: {
		    display: true,
		    text: 'Operation Proportion',
		    font: { size: 16 },
		},
		subtitle: {
		    display: true,
		    text: `${workload["request_distribution"]} distribution`,
		    position: 'bottom',
		    padding: 10,
		},
		tooltip: {
		    callbacks: {
			title: function (tooltipItems) {
			    return tooltipItems.label;
			},
			label: function (context) {
			    const point = context.raw;
			    return `${point * 100}%`;
			}
		    }
		}
	    }
	},
	data: {
	    labels: labels,
	    datasets: [{
		label: "Proportion",
		data: chartData,
		backgroundColor: [
		    colors.teal70,
		    colors.magenta70,
		    colors.cyan90,
		    colors.purple50,
		]
	    }]
	},
    });
    renderedChart["proportion"] = chart;
}

function loadFilterOperation(json) {
    const operations = getFilteredOperations(json);
    const successOperations = operations.filter(item => !item.includes("FAILED"));

    loadFilter(operationSelect, successOperations, "operation", null, renderLatencyThroughputChart, renderAbortRateChart, renderOperationCountChart);
}

function loadSelectLatency() {
    const latencies = [
	"AverageLatency(us)",
	"MinLatency(us)",
	"MaxLatency(us)",
	"50thPercentileLatency(us)",
	"95thPercentileLatency(us)",
	"99thPercentileLatency(us)"
    ];

    const radios = [];
    const labels = [];

    while (latencySelect.firstChild) latencySelect.removeChild(latencySelect.lastChild);
    latencies.forEach(item => {
	const label = document.createElement("label");
	label.classList.add("flex", "rounded-md", "px-2", "align-center", "border", "text-gray-950", "duration-100");
	label.setAttribute("data-val", item);

	if (filter["latency"] === item) {
	    label.classList.add("bg-sky-500", "border-sky-500", "hover:bg-sky-600");
	} else {
	    label.classList.add("border-gray-700", "hover:bg-gray-300");
	}

	const input = document.createElement("input");
	input.classList.add("cursor-pointer", "sr-only");
	input.setAttribute("type", "radio");
	input.setAttribute("name", "latency");
	input.setAttribute("value", item);

	const span = document.createElement("span");
	span.classList.add("text-xs");
	span.textContent = item.replace("(us)", "");

	label.append(input, span);
	latencySelect.appendChild(label);
	radios.push(input);
	labels.push(label);
    });

    const data = sessionStorage.getItem("data");
    const json = JSON.parse(data);
    radios.forEach(radio => {
	radio.addEventListener("click", (e) => {
	    if (filter["latency"] === e.target.value) return;

	    let prev = labels.find(item => item.getAttribute("data-val") === String(filter["latency"])); 
	    prev.classList.add("border-gray-700", "hover:bg-gray-300");
	    prev.classList.remove("bg-sky-500", "border-sky-500", "hover:bg-sky-600");

	    filter["latency"] = e.target.value;
	    let current = labels.find(item => item.getAttribute("data-val") === e.target.value); 
	    current.classList.add("bg-sky-500", "border-sky-500", "hover:bg-sky-600");
	    current.classList.remove("border-gray-700", "hover:bg-gray-300");

	    renderLatencyThroughputChart(json);
	});
    });

    let initial = labels.find(item => item.getAttribute("data-val") === String(filter["latency"])); 
    initial.classList.add("bg-sky-500", "border-sky-500", "hover:bg-sky-600");
    initial.classList.remove("border-gray-700", "hover:bg-gray-300");
}

function renderLatencyThroughputChart(json) {
    const filtered = [];
    const operation = filter["operation"];

    json.forEach(project => {
	project["protocols"].forEach(protocol => {
	    if (filter["protocol"] !== "All" && protocol["name"] !== filter["protocol"])
		return;

	    if (filter["consistency"] !== "All" && protocol["consistency"] !== filter["consistency"])
		return;

	    if (filter["persistency"] !== "All" && protocol["persistency"] !== filter["persistency"])
		return;

	    const projectItem = {
		project: project["project"],
		protocol: protocol["name"],
		commit: protocol["commit"],
	    }

	    protocol["workloads"].forEach(workload => {
		if (workload["name"] !== filter["workloadName"]
		    || workload["type"] !== filter["workloadType"]
		    || workload["operation_count"] !== filter["operationCount"]
		    || workload["record_count"] !== filter["recordCount"]
		) return;

		projectItem["result"] = [];
		workload["results"].forEach(result => {
		    const operationData = result["result"][operation];
		    if (!operationData) return;

		    const latencyData = operationData[filter["latency"]] / 1000.0;
		    const throughputData = result["result"]["OVERALL"]["Throughput(ops/sec)"];

		    projectItem["result"].push({
			threads: Number(result["thread_count"]),
			latency: latencyData,
			throughput: throughputData,
		    });
		});
	    });

	    if (!projectItem || !projectItem["result"] || projectItem["result"].length <= 0) return;
	    projectItem["result"].sort((a, b) => a["threads"] - b["threads"]);
	    filtered.push(projectItem);
	});
    });

    if (renderedChart["main"]) renderedChart["main"].destroy();
    const canvas = document.querySelector("canvas#result-chart");

    while (canvas.firstChild)
	canvas.removeChild(canvas.lastChild);

    const datasets = filtered.map((item, index) => {
	return {
	    label: `${item["project"]}-${item["protocol"]} (${item["commit"].slice(0, 7)})`,
	    data: item["result"].map(r => {
		return {
		    x: r["throughput"],
		    y: r["latency"],
		    customLabel: `${r["threads"]} threads`
		};
	    }),
	    borderColor: colorPalette[index % colorPalette.length],
	    borderWidth: 2,
	    fill: false,
	    tension: 0,
	};
    });
    const chart = new Chart(canvas, {
	type: 'line',
	data: {
	    datasets: datasets,
	},
	options: {
	    responsive: true,
	    maintainAspectRatio: false,
	    scales: {
		x: {
		    type: 'linear',
		    position: 'bottom',
		    title: {
			display: true,
			text: "Throughput (ops/sec)" 
		    },
		    grid: {
			color: bgColors.gray700,
			borderColor: bgColors.gray700,
			tickColor: bgColors.gray700,
		    },
		},
		y: {
		    title: {
			display: true,
			text: filter["latency"].replace("(us)", "(ms)")
		    },
		    grid: {
			color: bgColors.gray700,
			borderColor: bgColors.gray700,
			tickColor: bgColors.gray700,
		    },
		}
	    },
	    plugins: {
		tooltip: {
		    callbacks: {
			title: function (tooltipItems) {
			    const point = tooltipItems[0].raw;
			    return point.customLabel || '';
			},
			label: function (context) {
			    const point = context.raw;
			    const x = point.x;
			    const y = point.y;
			    const datasetLabel = context.dataset.label || '';
			    return [
				`Throughput: ${x.toFixed(2)} ops/sec`,
				`Latency: ${y.toFixed(2)} ms`,
				datasetLabel
			    ];
			}
		    }
		}
	    },
	},
    });

    renderedChart["main"] = chart;
}

function loadFilterThread(json) {
    const threadSet = new Set();

    json.forEach(project => {
	project["protocols"].forEach(protocol => {
	    protocol["workloads"].forEach(workload => {
		if (workload["name"] !== filter["workloadName"]) return;

		workload["results"].forEach(result => {
		    threadSet.add(Number(result["thread_count"]));
		});
	    });
	});
    });

    loadFilter(threadSelect, [...threadSet].sort((a, b) => a-b), "threads", null, renderOperationCountChart, renderAbortRateChart, loadTable, () => {
	tableTitle.textContent = `Overall Result (${filter["workloadName"]}, ${filter["threads"]} threads)`;
    });
}

function renderAbortRateChart(json) {
    // Get all operations in the workload
    const ycsbResults = getFilteredYcsbResult(json);

    if (renderedChart["abort-rate"]) renderedChart["abort-rate"].destroy();
    const canvas = document.querySelector("canvas#abort-rate-chart");

    while (canvas.firstChild) canvas.removeChild(canvas.lastChild);

    const data = ycsbResults.map(item => {
	const result = item["result"];
	const op = filter["operation"];

	const name = `${item["project"]}-${item["protocol"]} (${item.commit.slice(0, 7)})`;
	let okCount = result[op]["Return=OK"];
	let errCount = result[op]["Return=ERROR"];

	if (!okCount) okCount = 0;
	if (!errCount) errCount = 0;

	const value = errCount / (okCount + errCount) * 100;

	return { name: name, value: value };
    }).sort((a, b) => b.value - a.value);

    const chart = new Chart(canvas, {
	type: "bar",
	options: {
	    responsive: true,
	    maintainAspectRatio: false,
	    indexAxis: "y",
	    plugins: {
		title: {
		    display: true,
		    text: 'Abort Rate',
		    font: { size: 16 },
		},
		subtitle: {
		    display: true,
		    text: "Percentage of operations that failed",
		    position: "bottom",
		},
		tooltip: {
		    callbacks: {
			title: function (tooltipItems) {
			    return tooltipItems.label;
			},
			label: function (context) {
			    const point = context.raw;
			    return `${point.toFixed(2)}%`;
			}
		    }
		}
	    },
	    scales: {
		x: { grid: { color: bgColors.gray700, lineWidth: 1 } }, 
		y: { grid: { color: bgColors.gray700, lineWidth: 1 } }, 
	    },
	},
	data: {
	    labels: data.map(item => item.name),
	    datasets: [{
		label: `${filter["operation"]} Failure Rate`,
		data: data.map(item => item.value),
		backgroundColor: [colors.red90]
	    }]
	},
    });

    renderedChart["abort-rate"] = chart;
}

function renderOperationCountChart(json) {
    const filtered = getFilteredYcsbResult(json);

    if (renderedChart["op-distribution"]) renderedChart["op-distribution"].destroy();
    const canvas = document.querySelector("canvas#op-distribution-chart");

    while (canvas.firstChild) canvas.removeChild(canvas.lastChild);

    const operations = getFilteredOperations(json);

    const dataset = [];
    operations.forEach((op, index) => {
	const dataArr = [];

	filtered.forEach(item => {
	    const result = item["result"];
	    if (!result) return;

	    if (!Object.keys(result).includes(op))
		dataArr.push(0);

	    Object.entries(result).forEach(([key, obj]) => {
		if (key !== op) return;
		dataArr.push(obj["Operations"]);
	    });
	})

	const item = {
	    label: op,
	    data: dataArr,
	    backgroundColor: (
		(op === `${filter["operation"]}-FAILED`) ? 
		colors.red90 : colorPalette[index % colorPalette.length]
	    ),
	    fill: true,
	    maxBarThickness: 50,
	};

	dataset.push(item);
    });

    const chart = new Chart(canvas, {
	type: "bar",
	options: {
	    responsive: true,
	    maintainAspectRatio: false,
	    plugins: {
		title: {
		    display: true,
		    text: 'Operation Count',
		    font: { size: 16 },
		},
		subtitle: {
		    display: true,
		    text: "Number of operations in the benchmark",
		    position: "bottom",
		}
	    },
	    indexAxis: "y",
	    scales: {
		x: { 
		    grid: { color: bgColors.gray700, lineWidth: 1 },
		    stacked: true,
		}, 
		y: { 
		    grid: { color: bgColors.gray700, lineWidth: 1 },
		    stacked: true,
		}, 
	    },
	},
	data: {
	    labels: filtered.map(item => `${item.project}-${item.protocol} (${item.commit.slice(0, 7)})`),
	    datasets: dataset,
	},
    });
    const height = 100 + (60 * dataset.length);
    canvas.style["height"] = `${height}px`;

    renderedChart["op-distribution"] = chart;
}

function renderAbortRateThreadcountChart(json) {
    const filtered = [];

    json.forEach(project => {
	project["protocols"].forEach(protocol => {
	    if (filter["protocol"] !== "All" && protocol["name"] !== filter["protocol"])
		return;

	    if (filter["consistency"] !== "All" && protocol["consistency"] !== filter["consistency"])
		return;

	    if (filter["persistency"] !== "All" && protocol["persistency"] !== filter["persistency"])
		return;

	    protocol["workloads"].forEach(workload => {
		if (workload["name"] !== filter["workloadName"]
		    || workload["type"] !== filter["workloadType"]
		    || workload["operation_count"] !== filter["operationCount"]
		    || workload["record_count"] !== filter["recordCount"]
		) return;

		const projectItem = {
		    project: project["project"],
		    protocol: protocol["name"],
		    commit: protocol["commit"],
		}

		projectItem["results"] = workload["results"];
		filtered.push(projectItem);
	    });
	});
    });
    const threadSet = new Set();

    json.forEach(project => {
	project["protocols"].forEach(protocol => {
	    protocol["workloads"].forEach(workload => {
		if (workload["name"] !== filter["workloadName"]) return;

		workload["results"].forEach(result => {
		    threadSet.add(Number(result["thread_count"]));
		});
	    });
	});
    });

    if (renderedChart["abortRateThreadCount"]) renderedChart["abortRateThreadCount"].destroy();
    const canvas = document.querySelector("canvas#failure-threadcount-chart");
    while (canvas.firstChild) canvas.removeChild(canvas.lastChild);

    const threadList = [...threadSet].sort((a, b) => a-b);
    const datasets = filtered.map((item, index) => {
	return {
	    label: `${item["project"]}-${item["protocol"]} (${item["commit"].slice(0, 7)})`,
	    data: threadList.map(threadCount => {
		const resultItem = item["results"].find(r => r["thread_count"] === Number(threadCount));
		if (!resultItem) return { x: threadCount, y: 0 };

		const result = resultItem["result"]
		const op = filter["operation"];

		let okCount = result[op]["Return=OK"];
		let errCount = result[op]["Return=ERROR"];

		if (!okCount) okCount = 0;
		if (!errCount) errCount = 0;

		const abortRate = errCount / (okCount + errCount) * 100;

		return {
		    x: threadCount,
		    y: abortRate,
		};
	    }),
	    borderColor: colorPalette[index % colorPalette.length],
	    borderWidth: 2,
	    fill: false,
	    tension: 0,
	};
    });
    const chart = new Chart(canvas, {
	type: 'line',
	data: {
	    datasets: datasets,
	},
	options: {
	    responsive: true,
	    maintainAspectRatio: false,
	    scales: {
		x: {
		    type: 'linear',
		    position: 'bottom',
		    title: {
			display: true,
			text: "Number of Threads" 
		    },
		    grid: {
			color: bgColors.gray700,
			borderColor: bgColors.gray700,
			tickColor: bgColors.gray700,
		    },
		    ticks: {
			callback: function(value) {
			    if (threadList.includes(value))
				return value;
			},
			min: 2,
			max: threadList[-1],
			stepSize: 1,
		    },
		},
		y: {
		    title: {
			display: true,
			text: `${filter["operation"]} Abort Rate (%)`, 
		    },
		    grid: {
			color: bgColors.gray700,
			borderColor: bgColors.gray700,
			tickColor: bgColors.gray700,
		    },
		}
	    },
	    plugins: {
		title: {
		    display: true,
		    text: 'Abort Rate on All Thread Counts',
		    font: { size: 20 },
		},
		tooltip: {
		    callbacks: {
			title: function (tooltipItems) {
			    const point = tooltipItems[0].raw;
			    return point.customLabel || '';
			},
			label: function (context) {
			    const point = context.raw;
			    const x = point.x;
			    const y = point.y;
			    const datasetLabel = context.dataset.label || '';
			    return [
				`Abort Rate: ${y.toFixed(2)}%`,
				`Thread Count: ${x} threads`,
				datasetLabel
			    ];
			}
		    }
		}
	    },
	},
    });

    renderedChart["abortRateThreadCount"] = chart;
}

function loadTable(json) {
    const dataset = [];
    console.log(filter);
    json.forEach(project => {
	project.protocols.forEach(protocol => {
	    const selectedWorkload = protocol.workloads.find(workload => {
		return (workload["name"] === filter["workloadName"]
		    && workload["type"] === filter["workloadType"]
		    && workload["operation_count"] === filter["operationCount"]
		    && workload["record_count"] === filter["recordCount"]
		)
	    });

	    if (!selectedWorkload) return;
	    
	    const selectedResult = selectedWorkload.results.find(result => {
		return result["thread_count"] === Number(filter["threads"])
	    });

	    if (!selectedResult) return;

	    const protocolItem = {
		project: project.project,
		repo: project.repo,
		commit: protocol.commit,
		protocol: protocol.name,
		language: protocol.language,
		consistency: protocol.consistency,
		persistency: protocol.persistency,
		result: selectedResult.result["OVERALL"]
	    };

	    dataset.push(protocolItem);
	});
    });

    while (tableBody.firstChild) tableBody.removeChild(tableBody.lastChild);
    dataset.forEach(row => {
	const tr = document.createElement("tr");
	tr.classList.add("text-gray-950", "flex", "gap-5", "text-base", "text-left", "py-2", "px-4", "hover:bg-gray-300", "hover:duration-100");

	const project = document.createElement("td");
	project.classList.add("flex-2", "select-none", "basis-0", "w-0");
	project.textContent = row.project;

	const protocol = document.createElement("td");
	protocol.classList.add("flex-2", "select-none", "basis-0", "w-0");
	protocol.textContent = row.protocol;

	const commitHash = document.createElement("td");
	commitHash.classList.add("flex-1", "select-none", "basis-0", "w-0");
	commitHash.textContent = row.commit ? row.commit.slice(0, 7) : "-";

	const language = document.createElement("td");
	language.classList.add("flex-1", "select-none", "basis-0", "w-0");
	language.textContent = row.language ? row.language : "-";

	const runtime = document.createElement("td");
	runtime.classList.add("flex-1", "select-none", "basis-0", "w-0");
	runtime.textContent = row.result["RunTime(ms)"];

	const throughput = document.createElement("td");
	throughput.classList.add("flex-1", "select-none", "basis-0", "w-0");
	throughput.textContent = Number(row.result["Throughput(ops/sec)"]).toFixed(3);

	const consistency = document.createElement("td");
	consistency.classList.add("flex-1", "select-none", "basis-0", "w-0");
	consistency.textContent = row.consistency ? row.consistency : "-";

	const persistency = document.createElement("td");
	persistency.classList.add("flex-1", "select-none", "basis-0", "w-0");
	persistency.textContent = row.persistency ? row.persistency : "-";

	tr.append(project, protocol, commitHash, language, runtime, throughput, consistency, persistency);
	tr.addEventListener('click', () => {
	    window.open(`${row.repo.replace(".git", "")}/commit/${row.commit}`, '_blank');
	});
	tableBody.appendChild(tr);
    });
}


function sortTable(n) {
    var table, rows, switching, i, x, y, shouldSwitch, dir, switchcount = 0;
    table = document.getElementById("main-table");
    switching = true;

    dir = "asc";

    while (switching) {
	switching = false;
	rows = table.rows;

	for (i = 1; i < (rows.length - 1); i++) {
	    shouldSwitch = false;
	    x = rows[i].getElementsByTagName("TD")[n];
	    y = rows[i + 1].getElementsByTagName("TD")[n];

	    if (dir == "asc") {
		if (x.textContent.toLowerCase() > y.textContent.toLowerCase()) {
		    shouldSwitch = true;
		    break;
		}
	    } else if (dir == "desc") {
		if (x.textContent.toLowerCase() < y.textContent.toLowerCase()) {
		    shouldSwitch = true;
		    break;
		}
	    }
	}

	if (shouldSwitch) {
	    rows[i].parentNode.insertBefore(rows[i + 1], rows[i]);
	    switching = true;
	    switchcount ++;
	} else {
	    if (switchcount == 0 && dir == "asc") {
		dir = "desc";
		switching = true;
	    }
	}
    }
}


function main() {
    document.addEventListener("DOMContentLoaded", () => {
	console.log("dom loaded");
	let data = sessionStorage.getItem("data");

	if (!data) {
	    console.log("fetching from dom loaded");
	    data = fetchData("data.json");
	    sessionStorage.setItem("data", data);
	} else {
	    console.log("NOT fetching from dom loaded");
	}

	const json = JSON.parse(data);
	loadSelectWorkload(json);
	loadSelectWorkloadType(json);
	loadSelectNumNodes(json);
	loadSelectRecordCount(json);
	loadSelectOperationCount(json);
	loadFilterProtocol(json);
	loadSelectLatency();
	loadFilterConsistency(json);
	loadFilterPersistency(json);
	loadFilterOperation(json);
	loadFilterThread(json);

	const selectedWorkload = json[0]["protocols"][0]["workloads"][0];
	renderProportionChart(selectedWorkload);
	renderLatencyThroughputChart(json);
	renderOperationCountChart(json);
	renderAbortRateChart(json);
	renderAbortRateThreadcountChart(json);
	tableTitle.textContent = `Overall Result (${filter["workloadName"]}, ${filter["threads"]} threads)`;
	loadTable(json);

	// Workload Select
	workloadSelect.addEventListener("change", (e) => {
	    filter["workloadName"] = e.target.value;

	    let newWorkload = null;

	    json.forEach(project => {
		if (newWorkload) return;

		project["protocols"].forEach(protocol => {
		    if (newWorkload) return;

		    const foundItem = protocol["workloads"].find(w => w["name"] === filter["workloadName"]);
		    newWorkload = foundItem;
		});
	    });

	    loadFilterOperation(json);
	    renderProportionChart(newWorkload);
	    renderLatencyThroughputChart(json);
	    renderOperationCountChart(json);
	    renderAbortRateChart(json);
	    tableTitle.textContent = `Overall Result (${filter["workloadName"]}, ${filter["threads"]} threads)`;
	    loadTable(json);
	});
    });

    // Rerender Charts
    window.addEventListener('resize', () => {
	if (!renderedChart) return;

	Object.entries(renderedChart).forEach(([_, chart]) => {
	    chart.resize()
	});
    });
}

main();





/*
function loadWorkloadSelect(data) {

    while (workloadSelect.firstChild)
	workloadSelect.removeChild(workloadSelect.lastChild);

    workloads.forEach(item => {
	const opt = document.createElement("option");
	opt.setAttribute("value", item);
	opt.textContent = item["name"];
	workloadSelect.appendChild(opt);
    });
    workloadSelect.value = selectedWorkload;

    // Load Workload Types
    const workloadTypes = [...new Set(workloads.map(w => w["type"]))];
    while (workloadTypeSelect.firstChild)
	workloadTypeSelect.removeChild(workloadTypeSelect.lastChild);

    selectedWorkloadType = selectedWorkload["type"];
    workloadTypes.forEach(item => {
	const opt = document.createElement("option");
	opt.setAttribute("value", item);
	opt.textContent = item;
	workloadTypeSelect.appendChild(opt);
    });
    workloadTypeSelect.value = selectedWorkloadType;

    // Load Record Count
    const recordcount = [...new Set(workloads.map(w => w["record_count"]))];

    // Load Operation Count
    const operationcount = [...new Set(workloads.map(w => w["operation_count"]))];

    // Load Consistencies
    const consistencies = [...new Set(json.map(item => item.consistency))];


    // Load 
    selectedConsistency = params.get("consistency")

    if (
	!selectedConsistency ||
	!consistencies.includes(selectedConsistency)
    ) selectedConsistency = "All";

}

function loadMetricSelect(data) {
    if (!data) return;

    const json = JSON.parse(data).filter(item => item.workload === selectedWorkload);

    const keys = Object.keys(json[0].result);
    selectedMetric = "OVERALL";

    while (metricSelect.firstChild)
	metricSelect.removeChild(metricSelect.lastChild);

    keys.forEach(item => {
	const opt = document.createElement("option");
	opt.setAttribute("value", item);
	opt.textContent = item.toLowerCase();
	metricSelect.appendChild(opt);
    });

    metricSelect.value = selectedMetric;
    renderChart(data);
}

metricSelect.addEventListener("change", (e) => {
    selectedMetric = e.target.value;
    const data = sessionStorage.getItem("data");
    renderChart(data);
});


async function renderChart(data) {
    selectedMetric;
    const json = JSON.parse(data).filter(item => item.workload === selectedWorkload 
	&& (selectedProtocol !== "All" ? item.protocol === selectedProtocol : true)
	&& (selectedConsistency !== "All" ? item.consistency === selectedConsistency : true)
	&& (selectedPersistency !== "All" ? item.persistency === selectedPersistency : true)
    );

    if (prevChart.length > 0) {
	prevChart.forEach(chart => chart.destroy());
	prevChart = [];
    }

    if (selectedMetric === "OVERALL") {
	const chart1 = await createThroughputChart(json);
	const chart2 = await createRuntimeChart(json);
	chartSection.replaceChildren(chart1, chart2);
    } else if (selectedMetric === "READ" 
	| selectedMetric === "UPDATE") {
	const chart1 = await createLatencyPercentileChart(json);
	const chart2 = await createLatencyChart(json);
	chartSection.replaceChildren(chart1, chart2);
    }
}

async function createThroughputChart(json) {
    const container = document.createElement("div");
    container.classList.add("flex-1", "border", "border-gray-700", "rounded-lg", "overflow-hidden", "text-gray-950", "p-2");
    const canvas = document.createElement("canvas");
    canvas.id = "throughput";
    container.appendChild(canvas);

    const data = json.map(entry => {
	const name = `${entry.project} (${entry.protocol})`;
	const throughput = entry.result["OVERALL"]["Throughput(ops/sec)"];
	return { name: name, value: throughput }
    }).sort((a, b) => b.value - a.value);

    const chart = new Chart(canvas, {
	type: "bar",
	options: {
	    indexAxis: "y",
	    scales: {
		x: { grid: { color: bgColors.gray700, lineWidth: 1 } }, 
		y: { grid: { color: bgColors.gray700, lineWidth: 1 } }, 
	    },
	},
	data: {
	    labels: data.map(item => item.name),
	    datasets: [{
		label: "Throughput (ops/sec)",
		data: data.map(item => item.value),
		backgroundColor: [colors.purple70]
	    }]
	},
    });
    prevChart.push(chart);

    return container;
}

async function createRuntimeChart(json) {
    const container = document.createElement("div");
    container.classList.add("flex-1", "border", "border-gray-700", "rounded-lg", "overflow-hidden", "text-gray-950", "p-2");
    const canvas = document.createElement("canvas");
    canvas.id = "runtime";
    container.appendChild(canvas);

    const data = json.map(entry => {
	const name = `${entry.project} (${entry.protocol})`;
	const runtime = entry.result["OVERALL"]["RunTime(ms)"];
	return { name: name, value: runtime }
    }).sort((a, b) => a.value - b.value);

    const chart = new Chart(canvas, {
	type: "bar",
	options: {
	    indexAxis: "y",
	    scales: {
		x: { grid: { color: bgColors.gray700, lineWidth: 1 } }, 
		y: { grid: { color: bgColors.gray700, lineWidth: 1 } }, 
	    },
	},
	data: {
	    labels: data.map(item => item.name),
	    datasets: [{
		label: "RunTime (ms)",
		data: data.map(item => item.value),
		backgroundColor: [colors.red90]
	    }]
	},
    });
    prevChart.push(chart);
    return container;
}

async function createLatencyPercentileChart(json) {
    const container = document.createElement("div");
    container.classList.add("flex-1", "border", "border-gray-700", "rounded-lg", "overflow-hidden", "text-gray-950", "p-2");
    const canvas = document.createElement("canvas");
    canvas.id = "latency-percentile";
    container.appendChild(canvas);

    const data = json.map(entry => {
	const name = `${entry.project} (${entry.protocol})`;
	const p50 = entry.result[selectedMetric]["50thPercentileLatency(us)"];
	const p95 = entry.result[selectedMetric]["95thPercentileLatency(us)"];
	const p99 = entry.result[selectedMetric]["99thPercentileLatency(us)"];

	return { name: name, p50: p50, p95: p95, p99: p99 }
    }).sort((a, b) => a.name.localeCompare(b));
    const labels = ["50th Percentile", "95th Percentile", "99th Percentile"];

    const chart = new Chart(canvas, {
	type: "line",
	options: {
	    plugins: { 
		title: { text: "Latency Percentile (μs)", display: true, position: "bottom", font: { size: 18} },
		legend: { labels: { usePointStyle: true } }
	    },
	    scales: {
		x: { grid: { color: bgColors.gray700, lineWidth: 1 } }, 
		y: { grid: { color: bgColors.gray700, lineWidth: 1 } }, 
	    },
	},
	data: {
	    labels: labels,
	    datasets: data.map((item, index) => {
		return {
		    label: item.name,
		    data: [item.p50, item.p95, item.p99],
		    fill: false,
		    tension: 0.3,
		    pointStyle: 'rectRot',
		    pointRadius: 10,
		    borderColor: colorPalette[index % colorPalette.length],
		}
	    }),
	},
    });
    prevChart.push(chart);

    return container;
}

async function createLatencyChart(json) {
    const container = document.createElement("div");
    container.classList.add("flex-1", "border", "border-gray-700", "rounded-lg", "overflow-hidden", "text-gray-950", "p-2");
    const canvas = document.createElement("canvas");
    canvas.id = "latency";
    container.appendChild(canvas);

    const data = json.map(entry => {
	const name = `${entry.project} (${entry.protocol})`;
	const avg = entry.result[selectedMetric]["AverageLatency(us)"];
	const min = entry.result[selectedMetric]["MinLatency(us)"];
	const max = entry.result[selectedMetric]["MaxLatency(us)"];

	return { name: name, avg: avg, min: min, max: max }
    }).sort((a, b) => a.name.localeCompare(b));
    const labels = ["Minimum", "Average", "Maximum"];

    const chart = new Chart(canvas, {
	type: "line",
	options: {
	    plugins: { 
		title: { text: "Latency (μs)", display: true, position: "bottom", font: { size: 18}  },
		legend: { labels: { usePointStyle: true } } 
	    },
	    scales: {
		x: { grid: { color: bgColors.gray700, lineWidth: 1 } }, 
		y: { grid: { color: bgColors.gray700, lineWidth: 1 } }, 
	    },
	},
	data: {
	    labels: labels,
	    datasets: data.map((item, index) => {
		return {
		    label: item.name,
		    data: [item.min, item.avg, item.max],
		    fill: true,
		    tension: 0.3,
		    pointStyle: 'rectRot',
		    pointRadius: 10,
		    borderColor: colorPalette[index % colorPalette.length],
		}
	    }),
	},
    });
    prevChart.push(chart);

    return container;
}

function loadFilters(data) {
    const json = JSON.parse(data);
    loadProtocolFilter(json);
    loadConsistencyFilter(json);
    loadPersistencyFilter(json);
}

function loadProtocolFilter(json) {
    let protocols = [...new Set(json.map(item => item.protocol).filter(item => item !== ""))];
    protocols.push("All"); 
    const radios = [];
    const labels = [];

    while (protocolSelect.firstChild) protocolSelect.removeChild(protocolSelect.lastChild)
    protocols.forEach(item => {
	const label = document.createElement("label");
	label.classList.add("flex", "rounded-md", "px-2", "align-center", "border");
	label.setAttribute("data-val", item);
	if (selectedProtocol === item) {
	    label.classList.add("bg-sky-500", "text-gray-900", "border-sky-500");
	} else {
	    label.classList.add("border-gray-700", "text-gray-950", "hover:bg-gray-300");
	}

	const input = document.createElement("input");
	input.classList.add("cursor-pointer", "sr-only");
	input.setAttribute("type", "radio");
	input.setAttribute("name", "protocol");
	input.setAttribute("value", item);

	const span = document.createElement("span");
	span.classList.add("text-xs");
	span.textContent = item;

	label.append(input, span);
	protocolSelect.appendChild(label);
	radios.push(input);
	labels.push(label);
    });

    radios.forEach(radio => {
	radio.addEventListener("click", (e) => {
	    if (selectedProtocol === e.target.value) return;

	    let prev = labels.find(item => item.getAttribute("data-val") === selectedProtocol); 
	    console.log("prev", prev);
	    prev.classList.add("border-gray-700", "text-gray-950", "hover:bg-gray-300");
	    prev.classList.remove("bg-sky-500", "text-gray-950", "border-sky-500");

	    selectedProtocol = e.target.value;
	    let current = labels.find(item => item.getAttribute("data-val") === e.target.value); 
	    console.log("current", current);
	    current.classList.add("bg-sky-500", "text-gray-950", "border-sky-500");
	    current.classList.remove("border-gray-700", "text-gray-950", "hover:bg-gray-300");

	    const data = sessionStorage.getItem("data");
	    loadTable(data);
	    renderChart(data);
	});
    });
}

function loadConsistencyFilter(json) {
    let consistency = [...new Set(json.map(item => item.consistency).filter(item => item !== ""))];
    consistency.push("All"); 
    const radios = [];
    const labels = [];

    while (consistencySelect.firstChild) consistencySelect.removeChild(consistencySelect.lastChild)
    consistency.forEach(item => {
	const label = document.createElement("label");
	label.classList.add("flex", "rounded-md", "px-2", "align-center", "border");
	label.setAttribute("data-val", item);
	if (selectedConsistency === item) {
	    label.classList.add("bg-sky-500", "text-gray-950", "border-sky-500");
	} else {
	    label.classList.add("border-gray-700", "text-gray-950", "hover:bg-gray-800");
	}

	const input = document.createElement("input");
	input.classList.add("cursor-pointer", "sr-only");
	input.setAttribute("type", "radio");
	input.setAttribute("name", "consistency");
	input.setAttribute("value", item);

	const span = document.createElement("span");
	span.classList.add("text-xs");
	span.textContent = item;

	label.append(input, span);
	consistencySelect.appendChild(label);
	radios.push(input);
	labels.push(label);
    });

    radios.forEach(radio => {
	radio.addEventListener("click", (e) => {
	    if (selectedConsistency === e.target.value) return;

	    let prev = labels.find(item => item.getAttribute("data-val") === selectedConsistency); 
	    console.log("prev", prev);
	    prev.classList.add("border-gray-700", "text-gray-950", "hover:bg-gray-800");
	    prev.classList.remove("bg-sky-500", "text-gray-950", "border-sky-500");

	    selectedConsistency = e.target.value;
	    let current = labels.find(item => item.getAttribute("data-val") === e.target.value); 
	    console.log("current", current);
	    current.classList.add("bg-sky-500", "text-gray-950", "border-sky-500");
	    current.classList.remove("border-gray-700", "text-gray-950", "hover:bg-gray-800");

	    const data = sessionStorage.getItem("data");
	    loadTable(data);
	    renderChart(data);
	});
    });
}

function loadPersistencyFilter(json) {
    let persistency = [...new Set(json.map(item => item.persistency).filter(item => item !== ""))];
    persistency.push("All"); 
    const radios = [];
    const labels = [];

    while (persistencySelect.firstChild) persistencySelect.removeChild(persistencySelect.lastChild)
    persistency.forEach(item => {
	const label = document.createElement("label");
	label.classList.add("flex", "rounded-md", "px-2", "align-center", "border");
	label.setAttribute("data-val", item);
	if (selectedPersistency === item) {
	    label.classList.add("bg-sky-500", "text-gray-950", "border-sky-500");
	} else {
	    label.classList.add("border-gray-700", "text-gray-950", "hover:bg-gray-800");
	}

	const input = document.createElement("input");
	input.classList.add("cursor-pointer", "sr-only");
	input.setAttribute("type", "radio");
	input.setAttribute("name", "persistency");
	input.setAttribute("value", item);

	const span = document.createElement("span");
	span.classList.add("text-xs");
	span.textContent = item;

	label.append(input, span);
	persistencySelect.appendChild(label);
	radios.push(input);
	labels.push(label);
    });

    radios.forEach(radio => {
	radio.addEventListener("click", (e) => {
	    if (selectedPersistency === e.target.value) return;

	    let prev = labels.find(item => item.getAttribute("data-val") === selectedPersistency); 
	    prev.classList.add("border-gray-700", "text-gray-950", "hover:bg-gray-800");
	    prev.classList.remove("bg-sky-500", "text-gray-950", "border-sky-500");

	    selectedPersistency = e.target.value;
	    let current = labels.find(item => item.getAttribute("data-val") === e.target.value); 
	    current.classList.add("bg-sky-500", "text-gray-950", "border-sky-500");
	    current.classList.remove("border-gray-700", "text-gray-950", "hover:bg-gray-800");

	    const data = sessionStorage.getItem("data");
	    loadTable(data);
	    renderChart(data);
	});
    });
}
*/
