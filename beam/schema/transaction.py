import typing
from datetime import datetime


class Transaction(typing.NamedTuple):
    timestamp: datetime  # The id of the user who made the purchase.
    transaction_amount: float  # The identifier of the item that was purchased.
