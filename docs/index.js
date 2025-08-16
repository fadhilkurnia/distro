const workloadSelect = document.querySelector("select#workload-select");
const viewResults = document.querySelectorAll("a.view-result");
let selectedWorkload = "";
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

document.addEventListener("DOMContentLoaded", () => {
    const data = sessionStorage.getItem("data");
    viewResults.forEach(e => {
	const link = `/result.html?workload=${encodeURIComponent(selectedWorkload)}&consistency=${encodeURIComponent(e.getAttribute("data-consistency"))}`;
	e.setAttribute("href", link);
    });

    if (data) {
	loadWorkloadSelect(data);
	loadOverviewTables(data, selectedWorkload);
	return;
    }

    fetch("data.json").then(res => {
	    if (!res.ok) throw new Error('Network response was not OK');

	    return res.json();
	})
	.then(data => {
	    const renamed = data.map(item => {
		return {
		    ...item,
		    project: item.project.replaceAll(".", "/"),
		};
	    });
	    sessionStorage.setItem("data", JSON.stringify(renamed));

	    loadWorkloadSelect(renamed);
	    loadOverviewTables(renamed, selectedWorkload);
	})
	.catch(error => {
	    console.error('Error fetching file:', error);
	});
});


function loadWorkloadSelect(data) {
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
    const data = sessionStorage.getItem("data");
    loadOverviewTables(data, selectedWorkload);
    viewResults.forEach(e => {
	const link = `/result.html?workload=${encodeURIComponent(selectedWorkload)}&consistency=${encodeURIComponent(e.getAttribute("data-consistency"))}`;
	e.setAttribute("href", link);
    });
});


// Generate Overview Table
const linearizabiityTableBody = document.querySelector("table#overview-linearizability > tbody");
const primaryBackupTableBody = document.querySelector("table#overview-primary-backup > tbody");
const sequentialTableBody = document.querySelector("table#overview-sequential > tbody");
const causalTableBody = document.querySelector("table#overview-causal > tbody");
const pramTableBody = document.querySelector("table#overview-pram > tbody");
const eventualTableBody = document.querySelector("table#overview-eventual > tbody");

function loadOverviewTables(data, workload) {
    const filtered = JSON.parse(data).filter(item => item.workload === workload);

    const topLinearizable = filtered.filter(item => item.consistency === "Linearizability")
	.sort((a, b) => b.result["OVERALL"]["Throughput(ops/sec)"] - a.result["OVERALL"]["Throughput(ops/sec)"]).slice(0, 5);
    const topPrimaryBackup = filtered.filter(item => item.consistency.includes("Primary Integrity"))
	.sort((a, b) => b.result["OVERALL"]["Throughput(ops/sec)"] - a.result["OVERALL"]["Throughput(ops/sec)"]).slice(0, 5);
    const topSequential = filtered.filter(item => item.consistency === "Sequential")
	.sort((a, b) => b.result["OVERALL"]["Throughput(ops/sec)"] - a.result["OVERALL"]["Throughput(ops/sec)"]).slice(0, 5);
    const topCausal = filtered.filter(item => item.consistency === "Causal")
	.sort((a, b) => b.result["OVERALL"]["Throughput(ops/sec)"] - a.result["OVERALL"]["Throughput(ops/sec)"]).slice(0, 5);
    const topPram = filtered.filter(item => item.consistency === "Pram")
	.sort((a, b) => b.result["OVERALL"]["Throughput(ops/sec)"] - a.result["OVERALL"]["Throughput(ops/sec)"]).slice(0, 5);
    const topEventual = filtered.filter(item => item.consistency === "Eventual")
	.sort((a, b) => b.result["OVERALL"]["Throughput(ops/sec)"] - a.result["OVERALL"]["Throughput(ops/sec)"]).slice(0, 5);

    loadOverviewTable(linearizabiityTableBody, topLinearizable);
    loadOverviewTable(primaryBackupTableBody, topPrimaryBackup);
    loadOverviewTable(sequentialTableBody, topSequential);
    loadOverviewTable(causalTableBody, topCausal);
    loadOverviewTable(pramTableBody, topPram);
    loadOverviewTable(eventualTableBody, topEventual);
}

function loadOverviewTable(tablePtr, data) {
    while (tablePtr.firstChild)
	tablePtr.removeChild(tablePtr.lastChild)

    if (data === undefined || data === null || data.length === 0)
	return;

    data.forEach((item, index) => {
	const tr = document.createElement("tr");
	tr.classList.add("text-gray-100", "flex", "gap-5", "text-left", "py-2", "px-4", "hover:bg-gray-900", "hover:duration-100");

	const rank = document.createElement("td");
	rank.classList.add("w-10", "text-center");
	rank.textContent = index + 1;

	const protocol = document.createElement("td");
	protocol.classList.add("flex-3");
	protocol.textContent = `${item.protocol} - ${item.project}`;

	const runtime = document.createElement("td");
	runtime.classList.add("flex-1");
	runtime.textContent = item.result["OVERALL"]["RunTime(ms)"];


	const throughput = document.createElement("td");
	throughput.classList.add("flex-1");
	const thrRow = document.createElement("div");
	thrRow.classList.add("flex", "items-center", "gap-2");
	const thrBar1 = document.createElement("div");
	thrBar1.classList.add("w-15", "bg-gray-200", "rounded-full", "h-2", "dark:bg-gray-700");

	// round((max / curr) * 100)
	const max = data[0].result["OVERALL"]["Throughput(ops/sec)"];
	const curr = item.result["OVERALL"]["Throughput(ops/sec)"];
	const barWidth = Math.round(curr / max * 100);
	const thrBar2 = document.createElement("div");
	thrBar2.classList.add("bg-sky-600", "h-2", "rounded-full");
	thrBar2.style["width"] = `${barWidth}%`;

	thrBar1.appendChild(thrBar2);
	const sm = document.createElement("small");
	sm.textContent = curr;
	thrRow.appendChild(thrBar1, sm);
	throughput.appendChild(thrRow);

	tr.append(rank, protocol, runtime, throughput);	
	tablePtr.appendChild(tr);
    });
}
