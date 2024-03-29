import os
import re
import time
from datetime import datetime, timedelta

from cryptography import x509
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, serialization

PROXYPATH = f"/tmp/x509up_u{os.getuid()}"


def isOldProxy(cert):
    r"""Check if last CN is "proxy" or "limited proxy"."""
    lastCN = cert.subject.get_attributes_for_oid(x509.oid.NameOID.COMMON_NAME)[-1]
    return lastCN.value in ("proxy", "limited proxy")


def validKeyUsage(cert):
    """Check if digital signature bit is set in keyUsage extension."""
    try:
        keyUsage = cert.extensions.get_extension_for_oid(x509.oid.ExtensionOID.KEY_USAGE)
        return bool(keyUsage.value.digital_signature)
    except x509.ExtensionNotFound:
        return True


def createProxyCSR(issuerCert, proxyKey):
    """Create proxy certificate signing request."""

    if isOldProxy(issuerCert):
        raise Exception("Proxy format not supported")
    if not validKeyUsage(issuerCert):
        raise Exception("Proxy uses invalid keyUsage extension")

    builder = x509.CertificateSigningRequestBuilder()

    # copy subject to CSR
    subject = list(issuerCert.subject)
    builder = builder.subject_name(x509.Name(subject))

    # add proxyCertInfo extension
    oid = x509.ObjectIdentifier("1.3.6.1.5.5.7.1.14")
    value = b"0\x0c0\n\x06\x08+\x06\x01\x05\x05\x07\x15\x01"
    extension = x509.extensions.UnrecognizedExtension(oid, value)
    builder = builder.add_extension(extension, critical=True)

    # sign the proxy CSR with the issuer's private key
    csr = builder.sign(
        private_key=proxyKey,
        algorithm=hashes.SHA256(),
        backend=default_backend(),
    )

    return csr


def checkRFCProxy(proxy):
    """Check if valid X509 RFC 3820 proxy."""
    for ext in proxy.extensions:
        if ext.oid.dotted_string == "1.3.6.1.5.5.7.1.14":
            return True
    return False


def signRequest(csr, proxypath=PROXYPATH, lifetime=24):
    """Sign proxy CSR."""
    now = datetime.utcnow()
    if not csr.is_signature_valid:
        raise Exception("Invalid request signature")

    with open(proxypath, "rb") as f:
        proxy_pem = f.read()

    proxy = x509.load_pem_x509_certificate(proxy_pem, default_backend())
    if not checkRFCProxy(proxy):
        raise Exception("Invalid RFC proxy")

    key = serialization.load_pem_private_key(proxy_pem, password=None, backend=default_backend())
    keyID = x509.SubjectKeyIdentifier.from_public_key(key.public_key())

    subject = list(proxy.subject)
    subject.append(x509.NameAttribute(x509.oid.NameOID.COMMON_NAME, str(int(time.time()))))

    new_cert = x509.CertificateBuilder() \
                   .issuer_name(proxy.subject) \
                   .not_valid_before(now) \
                   .not_valid_after(now + timedelta(hours=lifetime)) \
                   .serial_number(proxy.serial_number) \
                   .public_key(csr.public_key()) \
                   .subject_name(x509.Name(subject)) \
                   .add_extension(x509.BasicConstraints(ca=False, path_length=None),
                                  critical=True) \
                   .add_extension(x509.KeyUsage(digital_signature=True,
                                                content_commitment=False,
                                                key_encipherment=False,
                                                data_encipherment=False,
                                                key_agreement=True,
                                                key_cert_sign=False,
                                                crl_sign=False,
                                                encipher_only=False,
                                                decipher_only=False),
                                  critical=True) \
                   .add_extension(x509.AuthorityKeyIdentifier(
                       key_identifier=keyID.digest,
                       authority_cert_issuer=[x509.DirectoryName(proxy.issuer)],
                       authority_cert_serial_number=proxy.serial_number
                       ),
                                  critical=False) \
                   .add_extension(x509.extensions.UnrecognizedExtension(
                       x509.ObjectIdentifier("1.3.6.1.5.5.7.1.14"),
                       b"0\x0c0\n\x06\x08+\x06\x01\x05\x05\x07\x15\x01"),
                                  critical=True) \
                   .sign(private_key=key,
                         algorithm=proxy.signature_hash_algorithm,
                         backend=default_backend())
    return new_cert.public_bytes(serialization.Encoding.PEM)


def parsePEM(pem):
    """Return a tuple of loaded cert, key and chain string from PEM."""
    sections = re.findall(
        "-----BEGIN.*?-----.*?-----END.*?-----",
        pem,
        flags=re.DOTALL
    )

    try:
        certPEM = sections[0]
        keyPEM = sections[1]
        chainPEMs = sections[2:]
    except IndexError:
        raise Exception("Invalid PEM")

    try:
        cert = x509.load_pem_x509_certificate(
            certPEM.encode(),
            default_backend()
        )
        key = serialization.load_pem_private_key(
            keyPEM.encode(),
            password=None,
            backend=default_backend()
        )
        for chainPEM in chainPEMs:
            x509.load_pem_x509_certificate(
                chainPEM.encode(),
                default_backend()
            )
        chain = "\n".join(chainPEMs)
    except ValueError:
        raise Exception("Cannot decode PEM")

    # return loaded cryptography objects and the issuer chain
    return cert, key, chain
