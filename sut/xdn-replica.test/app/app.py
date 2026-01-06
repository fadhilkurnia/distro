import logging
from flask import Flask, request, Response
import requests

# Configuration
# This is the address of your Docker container. 
# If running locally with `docker run -p 8080:80 nginx`, use localhost:8080
TARGET_URL = 'http://localhost:8080'
LISTEN_PORT = 5000

app = Flask(__name__)

# Configure logging to see the detection clearly in the console
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('ProxyDetector')

def detect_request(req):
    """
    Logic to handle detection. 
    You can expand this to save to a database, send a slack alert, etc.
    """
    logger.info(f" -> DETECTED REQUEST: {req.method} {req.path}")
    logger.info(f"    From IP: {req.remote_addr}")
    logger.info(f"    User Agent: {req.headers.get('User-Agent')}")
    logger.info("-" * 30)

@app.route('/', defaults={'path': ''}, methods=['GET', 'POST', 'PUT', 'DELETE', 'PATCH'])
@app.route('/<path:path>', methods=['GET', 'POST', 'PUT', 'DELETE', 'PATCH'])
def proxy(path):
    # 1. DETECT: Run our custom detection logic
    detect_request(request)

    # 2. PREPARE: specific headers might need to be stripped (like Host) 
    # so the upstream Nginx doesn't get confused about where the request came from.
    excluded_headers = ['Host', 'Content-Length']
    headers = {
        key: value for key, value in request.headers 
        if key not in excluded_headers
    }

    # 3. FORWARD: Send the request to the Docker container
    try:
        resp = requests.request(
            method=request.method,
            url=f"{TARGET_URL}/{path}",
            headers=headers,
            data=request.get_data(),
            cookies=request.cookies,
            params=request.args,
            allow_redirects=False # Let the client handle redirects
        )

        # 4. RETURN: Send the Docker container's response back to the client
        # We also need to filter hop-by-hop headers from the response
        excluded_response_headers = ['content-encoding', 'content-length', 'transfer-encoding', 'connection']
        response_headers = [
            (name, value) for (name, value) in resp.raw.headers.items()
            if name.lower() not in excluded_response_headers
        ]

        return Response(resp.content, resp.status_code, response_headers)

    except requests.exceptions.ConnectionError:
        logger.error(f"Failed to connect to Docker container at {TARGET_URL}")
        return Response("Error: Could not connect to the upstream Docker container.", 502)

if __name__ == '__main__':
    print(f"[*] Proxy starts on port {LISTEN_PORT}")
    print(f"[*] Forwarding to {TARGET_URL}")
    app.run(host='0.0.0.0', port=LISTEN_PORT, debug=False)
