import sys
import requests
from constants.terms import TIMESTAMP, CAUSAL_CONTEXT


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


def status_code_success(status_code: int) -> bool:
    """Determine if status code is a successful one

    Args:
        status_code (int)

    Returns:
        bool
    """
    return status_code >= 200 and status_code <= 300


def get_request_most_recent(responses: list) -> tuple:
    """Returns response and IP of response in list with lowest status code

    Args:
        responses (list): tuples with structure (response, IP address of response origin)

    Returns:
        tuple: response, IP address
    """

    def get_last_write_from_success_response(response):
        context = response.json().get(CAUSAL_CONTEXT)
        last_read = context[-1]
        return last_read[1].get(TIMESTAMP)

    success_responses = [
        response for response in responses if response[0].status_code == 200
    ]
    if len(success_responses):
        sort_by_last_write = sorted(
            success_responses,
            key=lambda r: get_last_write_from_success_response(r[0]),
            reverse=True,
        )
        return sort_by_last_write[0]
    else:
        sort_by_min_status = sorted(responses, key=lambda r: r[0].status_code)
        return sort_by_min_status[0]


def key_count_max(responses: list) -> int:
    """Allows for fetching the maximum key count in a set of responses (to mitigate gossip lag)

    Args:
        responses (list): tuples with structure (response, IP address of response origin)

    Returns:
        int: max key count
    """
    max_count = 0
    for response, _ in responses:
        json = response.json() or {}
        if json.get("key-count"):
            max_count = max(max_count, int(json.get("key-count")))

    return max_count