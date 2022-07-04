"""
This module defines all exceptions that are used by aCT.
"""


class InvalidJobDescriptionError(Exception):
    """Error if given job description is not valid."""
    pass


class NoSuchSiteError(Exception):
    """Error when site is not in configuration."""

    def __init__(self, siteName):
        """
        Initialize site name variable.

        Args:
            siteName: A string with name of site.
        """
        self.siteName = siteName


class UnknownClusterError(Exception):
    """Error when cluster is not in configuration."""

    def __init__(self, name):
        """
        Initialize cluster name variable.

        Args:
            name: A string with name of cluster.
        """
        self.name = name


class InvalidJobRangeError(Exception):
    """Error when job range is invalid."""

    def __init__(self, jobRange):
        """
        Initialize job range attribute.

        Args:
            jobRange: A string that is supposed to be range.
        """
        self.jobRange = jobRange


class InvalidJobIDError(Exception):
    """Error when job ID is not integer."""

    def __init__(self, jobid):
        """
        Initialize job ID attribute.

        Args:
            jobid: A value that is supposed to be ID.
        """
        self.jobid = jobid


class ConfigError(Exception):
    """Error when configuration parameter does not exist."""

    def __init__(self, name):
        self.name = name

    def __str__(self):
        return f"Configuration parameter {self.name} is not configured"


class NoJobDirectoryError(Exception):
    """Error when tmp job results directory does not exist."""

    def __init__(self, jobdir):
        """
        Initialize path attribute.

        Args:
            jobdir: A string with directory path where results should be.
        """
        self.jobdir = jobdir


class TargetDirExistsError(Exception):
    """Error when target directory for job already exists."""

    def __init__(self, dstdir):
        """
        Initialize path attribute.

        Args:
            dstdir: A string with existing destination directory path.
        """
        self.dstdir = dstdir


class NoSuchProxyError(Exception):
    """Error when proxy is not found in database."""

    def __init__(self, dn, attribute):
        """
        Initialize proxy attributes.

        Args:
            dn: A string with DN of proxy searched for.
            attribute: A string with proxy attributes of proxy searched for.
        """
        self.dn = dn
        self.attribute = attribute


class NoProxyFileError(Exception):
    """Error when given path is not a proxy file."""

    def __init__(self, path):
        """
        Initialize proxy attributes.

        Args:
            path: A string with path to a proxy file.
        """
        self.path = path


class InvalidColumnError(Exception):
    """Error when given column does not exist in database."""

    def __init__(self, name):
        self.name = name

    def __str__(self):
        return f'Invalid column "{self.name}"'


class ProxyFileExpiredError(Exception):
    """Error when expired proxy file is used."""
    pass


class ProxyDBExpiredError(Exception):
    """Error when db entry for proxy is expired while the file is not."""
    pass


class RESTError(Exception):
    """
    Base class for exceptions that have to be handled in endpoint routes.

    We want them to have error msg (already from base Exception), HTTP
    response code and potentially internal aCT error codes (which are not
    universally defined).
    """

    def __init__(self, msg, httpCode):
        self.msg = msg
        self.httpCode = httpCode

    def __str__(self):
        return self.msg
