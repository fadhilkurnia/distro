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
    green500: "#10b981",
};

let chart = null;
let systemName = null;
let systemData = null;

document.addEventListener("DOMContentLoaded", () => {
    loadSystemInfo();
});

async function loadSystemInfo() {
    try {
        const params = new URLSearchParams(window.location.search);
        systemName = params.get('system');

        if (!systemName) {
            throw new Error('No system specified in URL');
        }

        const dataResponse = await fetch('data.json');
        if (!dataResponse.ok) throw new Error('Failed to load data.json');

        const allData = await dataResponse.json();

        const systemEntry = allData.find(item => item.project === systemName);

        if (!systemEntry) {
            throw new Error(`System ${systemName} not found`);
        }

        if (!systemEntry.liveness_data) {
            throw new Error(`No liveness data available for ${systemName}`);
        }

        systemData = systemEntry;

        document.getElementById('systemName').textContent = systemEntry.project;

        await loadLivenessData(systemEntry.liveness_data);
    } catch (error) {
        console.error('Error loading system info:', error);
        document.querySelector('#livenessChart').insertAdjacentHTML('beforebegin',
            `<div class="text-red-400 text-center p-4">${error.message}</div>`
        );
    }
}

async function loadLivenessData(csvPath) {
    try {
        const response = await fetch(csvPath);
        if (!response.ok) throw new Error('Failed to load CSV file');

        const text = await response.text();
        const data = parseCSV(text);

        renderChart(data);
        updateStats(data);
    } catch (error) {
        console.error('Error loading liveness data:', error);
        throw error;
    }
}

function parseCSV(text) {
    const lines = text.trim().split('\n');
    const data = [];

    for (let i = 1; i < lines.length; i++) {
        const line = lines[i].trim();
        if (!line) continue;

        const [t, ops, latency] = line.split(',');
        data.push({
            time: parseInt(t) / 1000,
            ops: parseFloat(ops),
            latency: parseFloat(latency)
        });
    }

    return data;
}

function renderChart(data) {
    const canvas = document.getElementById('livenessChart');
    const ctx = canvas.getContext('2d');

    const gradient = ctx.createLinearGradient(0, 0, 0, 400);
    gradient.addColorStop(0, 'rgba(16, 185, 129, 0.6)');
    gradient.addColorStop(1, 'rgba(16, 185, 129, 0.05)');

    chart = new Chart(canvas, {
        type: 'line',
        data: {
            labels: data.map(d => d.time),
            datasets: [{
                label: 'Operations / Second',
                data: data.map(d => d.ops),
                borderColor: colors.green500,
                backgroundColor: gradient,
                borderWidth: 2,
                fill: true,
                tension: 0.1,
                pointRadius: 2,
                pointHoverRadius: 5,
                pointBackgroundColor: colors.green500,
                pointBorderColor: '#fff',
                pointBorderWidth: 1,
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    display: false
                },
                tooltip: {
                    backgroundColor: 'rgba(31, 41, 55, 0.95)',
                    borderColor: colors.gray700,
                    borderWidth: 2,
                    padding: 12,
                    displayColors: false,
                    callbacks: {
                        title: function(context) {
                            return `Time: ${context[0].label}s`;
                        },
                        label: function(context) {
                            const dataPoint = data[context.dataIndex];
                            const lines = [
                                `Throughput: ${dataPoint.ops} ops/sec`
                            ];

                            if (!isNaN(dataPoint.latency)) {
                                lines.push(`Latency: ${dataPoint.latency.toFixed(2)}ms`);
                            }

                            if (dataPoint.ops === 0) {
                                lines.push('⚠️ System unavailable');
                            }

                            return lines;
                        }
                    }
                }
            },
            scales: {
                x: {
                    type: 'linear',
                    title: {
                        display: true,
                        text: 'Time (seconds)',
                        color: colors.gray100,
                        font: {
                            size: 14,
                            weight: 'bold'
                        }
                    },
                    grid: {
                        color: colors.gray700,
                        lineWidth: 1
                    },
                    ticks: {
                        color: '#9ca3af',
                        stepSize: 5
                    }
                },
                y: {
                    beginAtZero: true,
                    title: {
                        display: true,
                        text: 'Operations / Second',
                        color: colors.gray100,
                        font: {
                            size: 14,
                            weight: 'bold'
                        }
                    },
                    grid: {
                        color: colors.gray700,
                        lineWidth: 1
                    },
                    ticks: {
                        color: '#9ca3af'
                    }
                }
            },
            interaction: {
                intersect: false,
                mode: 'index'
            }
        }
    });
}

function updateStats(data) {
    const nonZeroOps = data.filter(d => d.ops > 0).map(d => d.ops);
    const avgThroughput = nonZeroOps.length > 0
        ? (nonZeroOps.reduce((sum, val) => sum + val, 0) / nonZeroOps.length).toFixed(2)
        : 0;

    const maxThroughput = Math.max(...data.map(d => d.ops));

    let outageDuration = 0;
    let currentOutage = 0;
    let maxOutage = 0;

    for (let i = 0; i < data.length; i++) {
        if (data[i].ops === 0) {
            currentOutage++;
        } else {
            if (currentOutage > maxOutage) {
                maxOutage = currentOutage;
            }
            currentOutage = 0;
        }
    }

    if (currentOutage > maxOutage) {
        maxOutage = currentOutage;
    }

    outageDuration = maxOutage;

    document.getElementById('avgThroughput').textContent = avgThroughput;
    document.getElementById('maxThroughput').textContent = maxThroughput;
    document.getElementById('outageDuration').textContent = outageDuration;

    const outageDurationEl = document.getElementById('outageDuration');
    if (outageDuration === 0) {
        outageDurationEl.classList.remove('text-red-400');
        outageDurationEl.classList.add('text-green-400');
    } else if (outageDuration > 3) {
        outageDurationEl.classList.add('text-red-400');
    } else {
        outageDurationEl.classList.remove('text-red-400');
        outageDurationEl.classList.add('text-yellow-400');
    }
}

window.addEventListener('resize', () => {
    if (chart) {
        chart.resize();
    }
});
