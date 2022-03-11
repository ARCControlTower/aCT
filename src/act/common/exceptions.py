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
