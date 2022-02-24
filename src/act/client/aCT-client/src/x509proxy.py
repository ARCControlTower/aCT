import os
import time

from cryptography import x509
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from datetime import datetime, timedelta

import delegate_proxy


USERCERT = os.path.expandvars("$HOME/.globus/usercert.pem")
USERKEY = os.path.expandvars("$HOME/.globus/userkey.pem")
PROXYPATH = f"/tmp/x509up_u{os.getuid()}"


def create_proxy_csr(issuer_cert, proxy_key):
    """
    Create proxy certificate signing request.
    """

    # check that the issuer certificate is not an old proxy
    # and is using the keyUsage section as required
    delegate_proxy.confirm_not_old_proxy(issuer_cert)
    delegate_proxy.validate_key_usage(issuer_cert)

    builder = x509.CertificateSigningRequestBuilder()

    ## create a serial number for the new proxy
    ## Under RFC 3820 there are many ways to generate the serial number. However
    ## making the number unpredictable has security benefits, e.g. it can make
    ## this style of attack more difficult:
    ## http://www.win.tue.nl/hashclash/rogue-ca
    #serial = struct.unpack("<Q", os.urandom(8))[0]

    ## set the new proxy's subject
    ## append a CommonName to the new proxy's subject
    ## with the serial as the value of the CN
    #new_atribute = x509.NameAttribute(x509.oid.NameOID.COMMON_NAME, str(serial))
    subject_attributes = list(issuer_cert.subject)
    #subject_attributes.append(new_atribute)
    builder = builder.subject_name(x509.Name(subject_attributes))

    # add proxyCertInfo extension to the new proxy (We opt not to add keyUsage)
    # For RFC proxies the effective usage is defined as the intersection
    # of the usage of each cert in the chain. See section 4.2 of RFC 3820.

    # the constants 'oid' and 'value' are gotten from
    # examining output from a call to the open ssl function:
    # X509V3_EXT_conf(NULL, ctx, name, value)
    # ctx set by X509V3_set_nconf(&ctx, NCONF_new(NULL))
    # name = "proxyCertInfo"
    # value = "critical,language:Inherit all"
    oid = x509.ObjectIdentifier("1.3.6.1.5.5.7.1.14")
    value = b"0\x0c0\n\x06\x08+\x06\x01\x05\x05\x07\x15\x01"
    extension = x509.extensions.UnrecognizedExtension(oid, value)
    builder = builder.add_extension(extension, critical=True)

    # sign the new proxy with the issuer's private key
    csr = builder.sign(
        private_key=proxy_key,
        algorithm=hashes.SHA256(),
        backend=default_backend(),
    )

    # return CSR cryptography object
    return csr


def sign_proxy_csr(issuer_cert, issuer_key, csr, lifetime=12):
    """
    Sign proxy certificate signing request.

    Function copies information from CSR object to certificate builder and
    fills in some issuer information.
    """

    builder = x509.CertificateBuilder()

    # serial is value of the last object in the list of NameAttribute objects
    serial = int(list(csr.subject)[-1].value)
    builder = builder.serial_number(serial)

    # set the new proxy as valid from now until lifetime_hours have passed
    builder = builder.not_valid_before(datetime.datetime.utcnow())
    builder = builder.not_valid_after(datetime.datetime.utcnow() + datetime.timedelta(hours=lifetime))

    # copy public key from CSR
    builder = builder.public_key(csr.public_key())

    # set the issuer to the subject of the issuing cert
    builder = builder.issuer_name(issuer_cert.subject)

    # copy subject from CSR
    builder = builder.subject_name(csr.subject)

    # copy enxtensions from CSR
    for extension in csr.extensions:
        builder = builder.add_extension(extension.value, critical=extension.critical)

    # sign with the issuer's private key
    proxy_cert = builder.sign(
        private_key=issuer_key,
        algorithm=hashes.SHA256(),
        backend=default_backend(),
    )

    return proxy_cert


def check_rfc_proxy(proxy):
        """
        Check for X509 RFC 3820 proxy.
        """
        for ext in proxy.extensions:
            if ext.oid.dotted_string == "1.3.6.1.5.5.7.1.14":
                return True
        raise Exception('Invalid X509 RFC 3820 proxy.')


def sign_request(csr, lifetime=24):
    """
    Sign proxy.
    """
    now = datetime.utcnow()
    if not csr.is_signature_valid:
        raise ARCException('Invalid request signature.')

    with open(PROXYPATH,'rb') as f:
        proxy_pem=f.read()

    proxy = x509.load_pem_x509_certificate(proxy_pem, default_backend())

    oid=x509.ObjectIdentifier("1.3.6.1.4.1.8005.100.100.5")
    value=proxy.extensions.get_extension_for_oid(oid).value.value
    vomsext=x509.extensions.UnrecognizedExtension(oid,value)

    check_rfc_proxy(proxy)
    key = serialization.load_pem_private_key(proxy_pem, password=None, backend=default_backend())
    key_id = x509.SubjectKeyIdentifier.from_public_key(key.public_key())
    subject_attributes = list(proxy.subject)
    subject_attributes.append(
        x509.NameAttribute(x509.oid.NameOID.COMMON_NAME, str(int(time.time()))))

    new_cert = x509.CertificateBuilder() \
                   .issuer_name(proxy.subject) \
                   .not_valid_before(now) \
                   .not_valid_after(now + timedelta(hours=lifetime)) \
                   .serial_number(proxy.serial_number) \
                   .public_key(csr.public_key()) \
                   .subject_name(x509.Name(subject_attributes)) \
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
                       key_identifier=key_id.digest,
                       authority_cert_issuer=[x509.DirectoryName(proxy.issuer)],
                       authority_cert_serial_number=proxy.serial_number
                       ),
                                  critical=False) \
                   .add_extension(x509.extensions.UnrecognizedExtension(
                       x509.ObjectIdentifier("1.3.6.1.5.5.7.1.14"),
                       b"0\x0c0\n\x06\x08+\x06\x01\x05\x05\x07\x15\x01"),
                                  critical=True) \
                   .add_extension(vomsext,
                                  critical=False) \
                   .sign(private_key=key,
                         algorithm=proxy.signature_hash_algorithm,
                         backend=default_backend())
    return new_cert.public_bytes(serialization.Encoding.PEM)


def create_proxy_cert(issuer_cert, issuer_key, private_key, lifetime=12):
    csr = create_proxy_csr(issuer_cert, private_key)
    proxy_cert = sign_proxy_csr(issuer_cert, issuer_key, csr, lifetime)

    # return in PEM format as a unicode string
    return proxy_cert.public_bytes(serialization.Encoding.PEM).decode(
        "ascii")


# TODO: current logic only creates a proxy certificate from non proxy certificate
# and certificates, that don't have additional chain. The path is also hardcoded
# for source certificate.
if __name__ == "__main__":
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=1024,
        backend=default_backend()
    )

    #with open(USERCERT, "r") as f:
    #    issuer_cert = f.read()

    #with open(USERKEY, "r") as key_file:
    #    issuer_key = key_file.read()
    with open(PROXYPATH, "r") as key_file:
        issuer_cred = key_file.read()

    ## parse the issuer credential
    #issuer_cert = x509.load_pem_x509_certificate(issuer.encode("utf-8"), default_backend())
    #issuer_key = serialization.load_pem_private_key(issuer.encode("utf-8"), password=None, backend=default_backend())
    issuer_cert, issuer_key, issuer_chains = delegate_proxy.parse_issuer_cred(issuer_cred)

    csr = create_proxy_csr(issuer_cert, private_key)
    cert = sign_request(csr)
    #proxy = create_proxy_cert(issuer_cert, issuer_key, private_key)
    #chain = issuer_cert.public_bytes(serialization.Encoding.PEM).decode()

    key_pem = private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption()
    )

    #print proxy,
    #print key_pem,
    #print chain,

    chain = issuer_cert.public_bytes(serialization.Encoding.PEM).decode() + issuer_chains + "\n"
    #print(chain)

    with open("proxy.pem", "wb") as f:
        f.write(cert)
        f.write(key_pem)
        f.write(chain.encode())

    os.chmod(PROXYPATH, 0o600)
