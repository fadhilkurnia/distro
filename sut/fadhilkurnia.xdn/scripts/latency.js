import http from "k6/http";
import { check, sleep } from "k6";

// bookcatalog's REST API (per old.bookcatalog.pg.yaml's request routing):
// GET reads a book, POST/PUT/DELETE write. Distinct from Paxi's raw-body
// API — bookcatalog expects JSON and an XDN service-name header.
const ENDPOINT = "/api/books";

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
    const url = `http://${addr}${ENDPOINT}/1`;
    const res = http.get(url, { tags: { type: tagType, addr: addr } });
    check(res, { "status is 2xx": (r) => r.status >= 200 && r.status < 300 });
    sleep(REQUEST_INTERVAL);
}

function doWrite(addr, tagType) {
    requestCounter++;
    const url = `http://${addr}${ENDPOINT}`;
    const payload = JSON.stringify({
	title: `title${requestCounter}`,
	author: `client${__VU}`,
    });
    const res = http.post(url, payload, {
	headers: {
	    "Content-Type": "application/json",
	    "XDN": "bookcatalog",
	},
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
