import sys
import requests


def printer(msg: str):
    print(msg, file=sys.stdout, flush=True)


def request(
    url: str,
    method: str = "GET",
    headers: dict = {},
    json: dict = {},
) -> requests.Response:
    """Standard requests library wrapper

    Args:
        url (str)
        method (str, optional). Defaults to "GET".
        headers (dict, optional). Defaults to {}.
        json (dict, optional). Defaults to {}.

    Returns:
        requests.Response
    """
    url = "http://" + url
    headers.update({"Content-Type": "application/json"})
    return requests.request(
        method=method, url=url, headers=headers, json=json, timeout=3
    )


def status_code_success(status_code: int):
    return status_code >= 200 and status_code <= 300


def get_request_first_success(responses: list) -> tuple:
    min_status_response = None, None
    for response, ip in responses:
        if (
            min_status_response[0] == None
            or response.status_code < min_status_response[0].status_code
        ):
            min_status_response = response, ip
    return min_status_response


def key_count_max(responses: list) -> int:
    max_count = 0
    for response, _ in responses:
        json = response.json() or {}
        if json.get("key-count"):
            max_count = max(max_count, int(json.get("key-count")))

    return max_count