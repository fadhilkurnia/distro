const getDataDialog = document.querySelector("div#get-data");
const jsonInput = document.querySelector("input#json");
const getDataButton = document.querySelector("div#get-data button");
const tableBody = document.querySelector("table#main-table tbody");
const workloadSelect = document.querySelector("select#workload-select");
const metricSelect = document.querySelector("select#metric-select");
const chartSection = document.querySelector("div#chart-section");

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

document.addEventListener("DOMContentLoaded", function () {
    if (sessionStorage.getItem("data")) {
	loadWorkloadSelect();
	loadMetricSelect();
	loadTable();
	return;
    }

    getDataDialog.classList.remove("hidden");
    getDataDialog.classList.add("fixed");
});

getDataButton.addEventListener("click", () => {
    const file = jsonInput.files[0];

    if (file) {
	const reader = new FileReader();

	reader.onload = function (e) {
	    try {
		const json = JSON.parse(e.target.result);
		sessionStorage.setItem("data", JSON.stringify(json));

		getDataDialog.classList.add("fixed");
		getDataDialog.classList.add("hidden");
		
		loadWorkloadSelect();
		loadMetricSelect();
		loadTable();
	    } catch (err) {
		console.error("Invalid JSON:", err);
	    }
	};

	reader.readAsText(file);
    }
});

jsonInput.addEventListener("change", () => {
    getDataButton.disabled = jsonInput.files.length === 0;
});


function loadWorkloadSelect() {
    const data = sessionStorage.getItem("data");
    const json = JSON.parse(data);
    workloads = [...new Set(json.map(item => item.workload))];
    selectedWorkload = workloads[0];

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
    loadTable();
    loadMetricSelect();
});

function loadMetricSelect() {
    const data = sessionStorage.getItem("data");
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
    renderChart();
}

metricSelect.addEventListener("change", (e) => {
    selectedMetric = e.target.value;
    renderChart();
});


async function renderChart() {
    selectedMetric;

    const data = sessionStorage.getItem("data");
    const json = JSON.parse(data).filter(item => item.workload === selectedWorkload);

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

function loadTable() {
    const data = sessionStorage.getItem("data");
    const json = JSON.parse(data).filter(item => item.workload === selectedWorkload);

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
	language.textContent = "Go";

	const runtime = document.createElement("td");
	runtime.classList.add("flex-1", "select-none", "basis-0", "w-0");
	runtime.textContent = row.result.OVERALL["RunTime(ms)"];

	const throughput = document.createElement("td");
	throughput.classList.add("flex-2", "select-none", "basis-0", "w-0");
	throughput.textContent = row.result.OVERALL["Throughput(ops/sec)"];

	tr.append(project, protocol, language, runtime, throughput);
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
