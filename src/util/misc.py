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