class ACTError(Exception):
    """Base exception for aCT with a message."""

    def __init__(self, msg=""):
        self.msg = msg

    def __str__(self):
        return self.msg


class SubmitError(ACTError):
    """Submission errors with target state that jobs should be put in."""

    def __init__(self, arcstate, *args):
        super().__init__(*args)
        self.arcstate = arcstate


class ARCHTTPError(ACTError):
    """ARC REST HTTP status error."""

    def __init__(self, url, status, text):
        super().__init__(f"{url} {status} {text}")
        self.status = status
        self.text = text
