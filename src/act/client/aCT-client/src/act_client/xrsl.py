"""
Module implements parser for xRSL job description language.

There are probably edge cases where some character is not allowed or where some
characters are allowed that are illegal in the original specification. Also,
parsing of clientxrsl that has double quotes does not yet work. Updates to
regular expressions should be done. Also, operators are not working.

Parser returns job description in the form of a dictionary. Key is attribute
name and value is a list of values. List value can be a string or a list of
strings.

# Sample parser usage:
from xrsl import XRSLParser
parser = XRSLParser()
descs = parser.parse(xrslstr)
"""


from lark import Lark, Transformer


xRSLGrammar = r"""
    xrsl:     jobdesc | "+" "(" jobdesc+ ")"
    jobdesc:  "&" attrval+
    attrval:  "(" (quoted | attrname) "=" values ")"
    values:   (quoted | unquoted | valist)+
    valist:   "(" quoted+ ")"
    quoted:   STRING
    unquoted: /([A-Z]|[a-z]|[0-9]|\/|\\|-|_|\.|:|;|=)+/
    attrname: /([A-Z]|[a-z]|[0-9]|-|_)+/

    COMMENT:  /\(\*(.|\n)*?\*\)/

    %import python.STRING
    %import common.CNAME
    %import common.WS
    %ignore WS
    %ignore COMMENT
"""


class DescTransformer(Transformer):
    """Converts parsed tree into a list of description dicts."""

    def quoted(self, children):
        if not children:
            return ""
        else:
            return children[0]

    def STRING(self, children):
        return children[1:-1]

    def unquoted(self, children):
        return children[0].value

    def valist(self, children):
        return children

    def values(self, children):
        return children

    def attrname(self, children):
        return children[0].value

    def attrval(self, children):
        return (children[0].lower(), children[1])

    def jobdesc(self, children):
        desc = {}
        for attrname, attrval in children:
            desc[attrname] = attrval
        return desc

    def xrsl(self, children):
        return children


class XRSLParser:

    def __init__(self):
        self.parser = Lark(xRSLGrammar, parser="lalr", start="xrsl")
        self.transformer = DescTransformer()

    def parse(self, xrslstr):
        """
        Return a list of description dicts.

        A list is returned because xRSL can describe multiple jobs.
        """
        xrsltree = self.parser.parse(xrslstr)
        return self.transformer.transform(xrsltree)

    @staticmethod
    def _unparsePlainValueList(value_list):
        return " ".join([f"'{v}'" if '"' in v else f'"{v}"' for v in value_list])

    # saving space by not adding unnecessary whitespace
    @classmethod
    def _unparseSingleDesc(cls, desc):
        xrslstr = ""
        if len(desc) <= 0:
            return ""
        elif len(desc) >= 2:
            xrslstr += "&"

        for attrname, attrval in desc.items():
            xrslstr += f"({attrname}="
            if attrval and not isinstance(attrval[0], list):
                xrslstr += cls._unparsePlainValueList(attrval)
            else:
                xrslstr += "".join([f"({cls._unparsePlainValueList(value)})" for value in attrval])
            xrslstr += ")"

        return xrslstr

    @classmethod
    def unparse(cls, descs):
        if isinstance(descs, dict):
            return cls._unparseSingleDesc(descs)
        elif len(descs) <= 0:
            return None
        elif len(descs) == 1:
            return cls._unparseSingleDesc(descs[0])
        else:
            xrslstr = "+"
            for desc in descs:
                xrslstr += f"({cls._unparseSingleDesc(desc)})"
            return xrslstr
