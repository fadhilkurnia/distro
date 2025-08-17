const tableBody = document.querySelector("table#main-table tbody");
const workloadSelect = document.querySelector("select#workload-select");
const metricSelect = document.querySelector("select#metric-select");
const chartSection = document.querySelector("div#chart-section");

const protocolSelect = document.querySelector("#select-protocol");
const consistencySelect = document.querySelector("#select-consistency");
const persistencySelect = document.querySelector("#select-persistency");

let selectedProtocol = "All";
let selectedConsistency = "All";
let selectedPersistency = "All";

let selectedWorkload = "";
let selectedMetric = "";
let workloads = [];

const colors = {
    rose600: "#ec003f",
    pink600: "#e60076",
    indigo600: "#4f39f6",
    lime600: "#00a63e",
    yellow600: "#d08700",
    orange600: "#f54900",
    red600: "#e7000b",
    gray100: "#f3f4f6",
    gray700: "#364153",
};
const colorPalette = Object.values(colors);

let prevChart = [];
Chart.defaults.color = colors.gray100;

document.addEventListener("DOMContentLoaded", () => {
    const data = sessionStorage.getItem("data");

    if (data) {
	loadWorkloadSelect(data);
	loadMetricSelect(data);
	loadFilters(data);
	loadTable(data);
	return;
    }

    fetch("data.json").then(res => {
	    if (!res.ok) {
		throw new Error('Network response was not OK');
	    }
	    return res.json(); // Or .json() if JSON
	})
	.then(data => {
	    const renamed = data.map(item => {
		return {
		    ...item,
		    project: item.project.replaceAll(".", "/"),
		};
	    });
	    const renamedData = JSON.stringify(renamed);
	    sessionStorage.setItem("data", renamedData);

	    loadWorkloadSelect(renamedData);
	    loadMetricSelect(renamedData);
	    loadFilters(renamedData);
	    loadTable(renamedData);
	})
	.catch(error => {
	    console.error('Error fetching file:', error);
	});
});

function loadWorkloadSelect(data) {
    const json = JSON.parse(data);
    workloads = [...new Set(json.map(item => item.workload))];
    const consistencies = [...new Set(json.map(item => item.consistency))];

    const params = new URLSearchParams(window.location.search);
    selectedWorkload = params.get("workload");
    selectedConsistency = params.get("consistency")

    if (!selectedWorkload) selectedWorkload = workloads[0];
    if (
	!selectedConsistency ||
	!consistencies.includes(selectedConsistency)
    ) selectedConsistency = "All";

    // Load Workload Selection
    while (workloadSelect.firstChild)
	workloadSelect.removeChild(workloadSelect.lastChild);

    workloads.forEach(item => {
	const opt = document.createElement("option");
	opt.setAttribute("value", item);
	opt.textContent = item;
	workloadSelect.appendChild(opt);
    });

    workloadSelect.value = selectedWorkload;
}

workloadSelect.addEventListener("change", (e) => {
    selectedWorkload = e.target.value;
    const data = sessionStorage.getItem("data");
    loadTable(data);
    renderChart(data);
});

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
    container.classList.add("flex-1", "border", "border-gray-700", "rounded-lg", "overflow-hidden", "text-gray-100", "p-2");
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
		x: { grid: { color: colors.gray700, lineWidth: 1 } }, 
		y: { grid: { color: colors.gray700, lineWidth: 1 } }, 
	    },
	},
	data: {
	    labels: data.map(item => item.name),
	    datasets: [{
		label: "Throughput (ops/sec)",
		data: data.map(item => item.value),
		backgroundColor: [colors.rose600]
	    }]
	},
    });
    prevChart.push(chart);

    return container;
}

async function createRuntimeChart(json) {
    const container = document.createElement("div");
    container.classList.add("flex-1", "border", "border-gray-700", "rounded-lg", "overflow-hidden", "text-gray-100", "p-2");
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
		x: { grid: { color: colors.gray700, lineWidth: 1 } }, 
		y: { grid: { color: colors.gray700, lineWidth: 1 } }, 
	    },
	},
	data: {
	    labels: data.map(item => item.name),
	    datasets: [{
		label: "RunTime (ms)",
		data: data.map(item => item.value),
		backgroundColor: [colors.pink600]
	    }]
	},
    });
    prevChart.push(chart);
    return container;
}

async function createLatencyPercentileChart(json) {
    const container = document.createElement("div");
    container.classList.add("flex-1", "border", "border-gray-700", "rounded-lg", "overflow-hidden", "text-gray-100", "p-2");
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
		x: { grid: { color: colors.gray700, lineWidth: 1 } }, 
		y: { grid: { color: colors.gray700, lineWidth: 1 } }, 
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
    container.classList.add("flex-1", "border", "border-gray-700", "rounded-lg", "overflow-hidden", "text-gray-100", "p-2");
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
		x: { grid: { color: colors.gray700, lineWidth: 1 } }, 
		y: { grid: { color: colors.gray700, lineWidth: 1 } }, 
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

function loadTable(data) {
    const json = JSON.parse(data).filter(item => item.workload === selectedWorkload 
	&& (selectedProtocol !== "All" ? item.protocol === selectedProtocol : true)
	&& (selectedConsistency !== "All" ? item.consistency === selectedConsistency : true)
	&& (selectedPersistency !== "All" ? item.persistency === selectedPersistency : true)
    );

    while (tableBody.firstChild) {
	tableBody.removeChild(tableBody.lastChild);
    }

    json.forEach(row => {
	const tr = document.createElement("tr");
	tr.classList.add("text-gray-100", "flex", "gap-5", "text-base", "text-left", "py-2", "px-4", "hover:bg-gray-900", "hover:duration-100");

	const project = document.createElement("td");
	project.classList.add("flex-2", "select-none", "basis-0", "w-0");
	project.textContent = row.project;

	const protocol = document.createElement("td");
	protocol.classList.add("flex-2", "select-none", "basis-0", "w-0");
	protocol.textContent = row.protocol;

	const language = document.createElement("td");
	language.classList.add("flex-1", "select-none", "basis-0", "w-0");
	language.textContent = row.language ? row.language : "-";

	const runtime = document.createElement("td");
	runtime.classList.add("flex-1", "select-none", "basis-0", "w-0");
	runtime.textContent = row.result.OVERALL["RunTime(ms)"];

	const throughput = document.createElement("td");
	throughput.classList.add("flex-1", "select-none", "basis-0", "w-0");
	throughput.textContent = Number(row.result.OVERALL["Throughput(ops/sec)"]).toFixed(3);

	const consistency = document.createElement("td");
	consistency.classList.add("flex-1", "select-none", "basis-0", "w-0");
	consistency.textContent = row.consistency ? row.consistency : "-";

	const persistency = document.createElement("td");
	persistency.classList.add("flex-1", "select-none", "basis-0", "w-0");
	persistency.textContent = row.persistency ? row.persistency : "-";

	tr.append(project, protocol, language, runtime, throughput, consistency, persistency);
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

window.addEventListener('resize', () => {
    if (!prevChart) return;
    prevChart.forEach(chart => chart.resize());
});

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
	    label.classList.add("bg-sky-500", "text-gray-950", "border-sky-500");
	} else {
	    label.classList.add("border-gray-700", "text-gray-100", "hover:bg-gray-800");
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
	    prev.classList.add("border-gray-700", "text-gray-100", "hover:bg-gray-800");
	    prev.classList.remove("bg-sky-500", "text-gray-950", "border-sky-500");

	    selectedProtocol = e.target.value;
	    let current = labels.find(item => item.getAttribute("data-val") === e.target.value); 
	    console.log("current", current);
	    current.classList.add("bg-sky-500", "text-gray-950", "border-sky-500");
	    current.classList.remove("border-gray-700", "text-gray-100", "hover:bg-gray-800");

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
	    label.classList.add("border-gray-700", "text-gray-100", "hover:bg-gray-800");
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
	    prev.classList.add("border-gray-700", "text-gray-100", "hover:bg-gray-800");
	    prev.classList.remove("bg-sky-500", "text-gray-950", "border-sky-500");

	    selectedConsistency = e.target.value;
	    let current = labels.find(item => item.getAttribute("data-val") === e.target.value); 
	    console.log("current", current);
	    current.classList.add("bg-sky-500", "text-gray-950", "border-sky-500");
	    current.classList.remove("border-gray-700", "text-gray-100", "hover:bg-gray-800");

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
	    label.classList.add("border-gray-700", "text-gray-100", "hover:bg-gray-800");
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
	    prev.classList.add("border-gray-700", "text-gray-100", "hover:bg-gray-800");
	    prev.classList.remove("bg-sky-500", "text-gray-950", "border-sky-500");

	    selectedPersistency = e.target.value;
	    let current = labels.find(item => item.getAttribute("data-val") === e.target.value); 
	    current.classList.add("bg-sky-500", "text-gray-950", "border-sky-500");
	    current.classList.remove("border-gray-700", "text-gray-100", "hover:bg-gray-800");

	    const data = sessionStorage.getItem("data");
	    loadTable(data);
	    renderChart(data);
	});
    });
}
