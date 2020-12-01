import typing
import requests
from util.misc import status_code_success


class GetResponse(typing.NamedTuple):
    """
    Response interface for GET requests
    """

    status_code: int = None
    context: dict = {}
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
        )


class PutResponse(typing.NamedTuple):
    """
    Response interface for PUT requests
    """

    status_code: int = None
    context: dict = {}
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
