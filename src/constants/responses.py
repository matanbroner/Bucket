import typing
import requests
from util.misc import status_code_success


def success_response(msg: str = "Success") -> tuple:
    """Generic success response

    Args:
        msg (str, optional). Defaults to "Success".

    Returns:
        tuple: message, status code
    """
    return msg, 200


def key_count_response(count: int, shard_id: int) -> tuple:
    """Response from call to /kvs/key-count

    Args:
        count (int): key count
        shard_id (int)

    Returns:
        tuple: json, status code
    """
    return {
        "message": "Key count retrieved successfully",
        "key-count": count,
        "shard-id": shard_id,
    }, 200


def all_shards_info_response(shards: list) -> tuple:
    """Response from call to /kvs/shards

    Args:
        shards (list): list of shard ID's
    Returns:
        tuple: json, status code
    """
    return {"message": "Shard membership retrieved successfully", "shards": shards}, 200


def single_shard_info_response(count: int, shard_id: int, replicas: list) -> tuple:
    """Response from call to /kvs/shards/{id}

    Args:
        count (int): key count
        shard_id (int)
        replicas (list): list of IP's in shard

    Returns:
        tuple: json, status code
    """
    return {
        "message": "Shard information retrieved successfully",
        "shard-id": shard_id,
        "key-count": count,
        "replicas": replicas,
    }


def view_change_response(template: dict):
    """Response from call to /kvs/view-change

    Args:
        template (dict)

    Returns:
        tuple: json, status code
    """
    return {"shards": template, "message": "View change successful"}, 200


class GetResponse(typing.NamedTuple):
    """
    Response interface for GET requests
    """

    status_code: int = None
    context: list = []
    address: str = None
    value: str = None
    message: str = None
    error: str = None

    def to_flask_response(self, include_address: bool = True) -> tuple:
        """Transform class into expected JSON serialization tuple for client response

        Args:
            include_address (bool, optional): should address be included in sent JSON. Defaults to True.

        Returns:
            tuple: JSON, status code
        """
        json = {}
        if (
            self.status_code == 503 or self.status_code == 400
        ):  # timeout or causal context error
            json["message"] = "Error in GET"
            json["error"] = self.error
            return json, self.status_code
        elif status_code_success(self.status_code):
            json["message"] = self.message
            json["value"] = self.value
            json["doesExist"] = True
        else:
            json["error"] = self.error
            json["message"] = "Error in GET"
            json["doesExist"] = False
        if include_address:
            json["address"] = self.address
        json["causal-context"] = self.context
        return json, self.status_code

    @classmethod
    def from_flask_response(
        cls, response: requests.Response, manual_address: str = None
    ):
        """Generate response instance from a Flask request.Response instance

        Args:
            response (requests.Response)
            manual_address (str): address of responder

        Returns:
            GetResponse
        """
        status_code = response.status_code
        json = response.json()
        value, context, address, message, error = (
            json.get("value"),
            json.get("causal-context"),
            json.get("address") or manual_address,
            json.get("message"),
            json.get("error"),
        )
        return cls(
            value=value,
            status_code=status_code,
            context=context,
            address=address,
            message=message,
            error=error,
        )


class PutResponse(typing.NamedTuple):
    """
    Response interface for PUT requests
    """

    status_code: int = None
    context: list = []
    address: str = None
    message: str = None
    error: str = None

    def to_flask_response(self, include_address: bool = True):
        """Transform class into expected JSON serialization tuple for client response

        Args:
            include_address (bool, optional): should address be included in sent JSON. Defaults to True.

        Returns:
            tuple: JSON, status code
        """
        json = {}
        if self.status_code == 503:  # timeout or causal context error
            json["message"] = "Error in PUT"
            json["error"] = self.error
            return json, self.status_code
        elif status_code_success(self.status_code):
            json["message"] = self.message
            json["replaced"] = self.status_code == 200
        else:
            json["error"] = self.error
            json["message"] = "Error in PUT"
        if include_address:
            json["address"] = self.address
        json["causal-context"] = self.context
        return json, self.status_code

    @classmethod
    def from_flask_response(
        cls, response: requests.Response, manual_address: str = None
    ):
        """Generate response instance from a Flask request.Response instance

        Args:
            response (requests.Response)
            manual_address (str): address of responder

        Returns:
            PutResponse
        """
        status_code = response.status_code
        json = response.json()
        context, address, message, error = (
            json.get("causal-context"),
            json.get("address") or manual_address,
            json.get("message"),
            json.get("error"),
        )
        return cls(
            status_code=status_code,
            context=context,
            address=address,
            message=message,
            error=error,
        )
