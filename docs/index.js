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

async function fetchData(filename) {
    const res = await fetch(filename);
    if (!res.ok) throw new Error("Network response was not OK");

    const json = await res.json();
    const renamed = json.map(item => {
	return { ...item, project: item.project.replaceAll(".", "/") }
    });

    const newData = JSON.stringify(renamed);
    return newData;
}

const filter = {
    num_of_nodes: 3,
    workloadName: "Workload B: Read mostly workload",
    workloadType: "single-client",
    latency: "AverageLatency(us)",
    threads: 64,
    operationCount: 500000,
    recordCount: 1000000,
};

function preprocessData(json) {
    console.log("preprocessData");

    const data = [];
    json.forEach(project => {
	project.protocols.forEach(protocol => {
	    protocol.workloads.forEach(workload => {
		if (workload.name !== filter.workloadName
		    || workload.type !== filter.workloadType
		    || workload.num_of_nodes !== filter.num_of_nodes
		    || Number(workload.operation_count) !== filter.operationCount
		    || Number(workload.record_count) !== filter.recordCount)
		    return;

		workload.results.forEach(item => {
		    if (item.thread_count !== filter.threads) return;

		    const result = item.result;
		    const operationSet = new Set();
		    Object.keys(result).forEach(key => { 
			if (key !== "OVERALL" && !key.includes("FAILED"))
			    operationSet.add(key)
		    });

		    let avgLatency = 0;
		    [...operationSet].forEach(op => {
			avgLatency += result[op][filter.latency] * result[op]["Operations"] / filter.operationCount;
		    });

		    data.push({
			protocol: `${protocol.name} - ${project.project} (${protocol.commit.slice(0, 7)})`,
			consistency: protocol.consistency,
			throughput: Number(result.OVERALL["Throughput(ops/sec)"]),
			avgLatency: avgLatency,
		    });
		});
	    });
	});
    });

    return data;
}

function loadTable(bodyPtr, data) {
    // Assumes the data is already ordered by rank.
    while (bodyPtr.firstChild) bodyPtr.removeChild(bodyPtr.lastChild);

    if (data.length < 1) return;

    const maxThroughput = data[0].throughput;
    data.forEach((item, i) => {
	const tr = document.createElement("tr");
	tr.classList.add("text-gray-950", "flex", "gap-5", "text-left", "py-2", "px-4", "hover:bg-gray-300", "hover:duration-100");

	const rank = document.createElement("td");
	rank.classList.add("w-10", "text-center");
	rank.textContent = i+1;

	const protocol = document.createElement("td");
	protocol.classList.add("flex-3");
	protocol.textContent = item.protocol;

	const latency = document.createElement("td");
	latency.classList.add("flex-1");
	latency.textContent = item.avgLatency.toFixed(0);

	const throughput = document.createElement("td");
	throughput.classList.add("flex-1");
	const thrDiv = document.createElement("div");
	thrDiv.classList.add("flex", "items-center", "gap-2");

	const thrBarBg = document.createElement("div");
	thrBarBg.classList.add("w-15", "rounded-full", "h-2", "bg-gray-700");
	const thrBar = document.createElement("div");
	thrBar.classList.add("bg-sky-600", "h-2", "rounded-full");
	thrBar.style["width"] = `${item.throughput / maxThroughput * 100}%`;
	thrBarBg.appendChild(thrBar);

	const small = document.createElement("small");
	small.textContent = item.throughput.toFixed(0);

	thrDiv.append(thrBarBg, small);
	throughput.appendChild(thrDiv);
	tr.append(rank, protocol, latency, throughput);
	bodyPtr.appendChild(tr);
    });
}

function loadLinearizabilityTable(json) {
    const filter = json.filter(item => item.consistency === "Linearizability");
    const ranked = filter.sort((a, b) => (1.0 * b.throughput / b.avgLatency) - (1.0 * a.throughput / a.avgLatency));

    const tableBody = document.querySelector("table#overview-linearizability > tbody");
    loadTable(tableBody, ranked.slice(0, 5));
}

function loadPrimaryBackupTable(json) {
    const filter = json.filter(item => item.consistency === "Linearizability + Primary Integrity");
    const ranked = filter.sort((a, b) => (1.0 * b.throughput / b.avgLatency) - (1.0 * a.throughput / a.avgLatency));

    const tableBody = document.querySelector("table#overview-primary-backup > tbody");
    loadTable(tableBody, ranked.slice(0, 5));
}

function loadSequentialTable(json) {
    const filter = json.filter(item => item.consistency === "Sequential");
    const ranked = filter.sort((a, b) => (1.0 * b.throughput / b.avgLatency) - (1.0 * a.throughput / a.avgLatency));

    const tableBody = document.querySelector("table#overview-sequential > tbody");
    loadTable(tableBody, ranked.slice(0, 5));
}

function loadCausalTable(json) {
    const filter = json.filter(item => item.consistency === "Causal");
    const ranked = filter.sort((a, b) => (1.0 * b.throughput / b.avgLatency) - (1.0 * a.throughput / a.avgLatency));

    const tableBody = document.querySelector("table#overview-causal > tbody");
    loadTable(tableBody, ranked.slice(0, 5));
}

function loadPramTable(json) {
    const filter = json.filter(item => item.consistency === "Pram");
    const ranked = filter.sort((a, b) => (1.0 * b.throughput / b.avgLatency) - (1.0 * a.throughput / a.avgLatency));

    const tableBody = document.querySelector("table#overview-pram > tbody");
    loadTable(tableBody, ranked.slice(0, 5));
}

function loadEventualTable(json) {
    const filter = json.filter(item => item.consistency === "Eventual");
    const ranked = filter.sort((a, b) => (1.0 * b.throughput / b.avgLatency) - (1.0 * a.throughput / a.avgLatency));

    const tableBody = document.querySelector("table#overview-eventual > tbody");
    loadTable(tableBody, ranked.slice(0, 5));
}

function main() {
    document.addEventListener("DOMContentLoaded", async () => {
	let data = sessionStorage.getItem("data");

	if (!data) {
	    data = await fetchData("data.json");
	    sessionStorage.setItem("data", data);
	}
	const json = preprocessData(JSON.parse(data));
	loadLinearizabilityTable(json);
	loadPrimaryBackupTable(json);
	loadSequentialTable(json);
	loadCausalTable(json);
	loadPramTable(json);
	loadEventualTable(json);
    });
}

main();
