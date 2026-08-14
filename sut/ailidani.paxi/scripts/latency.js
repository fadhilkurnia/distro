import http from "k6/http";
import { check, sleep } from "k6";

// Paxi's actual REST API (per its own README): GET reads a key, POST
// writes a key with the request body as the raw value — no JSON, no
// custom headers, unlike some other protocols' APIs.
const KEY = "/";

const WRITE_RATIO = parseFloat(__ENV.WRITE_RATIO);
const REQUEST_INTERVAL = parseFloat(__ENV.REQUEST_INTERVAL);

let requestCounter = 0;

export const options = {
    scenarios: {
	/* SCENARIOS */
    },
    thresholds: {
	http_req_duration: ["p(50)<500", "p(99)<2000"],
    },
};

function doRead(addr, tagType) {
    const url = `http://${addr}${KEY}`;
    console.log(`read-to: ${url}`);
    const res = http.get(url, { tags: { type: tagType, addr: addr } });
    check(res, { "status is 2xx": (r) => r.status >= 200 && r.status < 300 });
    sleep(REQUEST_INTERVAL);
}

function doWrite(addr, tagType) {
    requestCounter++;
    const url = `http://${addr}${KEY}`;
    console.log(`write-to: ${url}`);
    const res = http.post(url, `value-${requestCounter}`, {
	tags: { type: tagType, addr: addr },
    });
    check(res, { "status is 2xx": (r) => r.status >= 200 && r.status < 300 });
    sleep(REQUEST_INTERVAL);
}

export function warmup() {
    const isWrite = Math.random() < WRITE_RATIO;
    if (isWrite) {
	doWrite(__ENV.ADDR, "write");
    } else {
	doRead(__ENV.ADDR, "read");
    }
}

export function benchmark() {
    const isWrite = Math.random() < WRITE_RATIO;
    if (isWrite) {
	doWrite(__ENV.ADDR, "write");
    } else {
	doRead(__ENV.ADDR, "read");
    }
}
