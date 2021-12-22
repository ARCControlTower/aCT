import os
import argparse
import sys
import requests
import x509proxy

from config import loadConf, checkConf, expandPaths
from common import readProxyFile, addCommonArgs
from delegate_proxy import parse_issuer_cred
from cryptography import x509
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization


def main():
    parser = argparse.ArgumentParser(description='Submit proxy to aCT server')
    addCommonArgs(parser)
    args = parser.parse_args()

    conf = loadConf(path=args.conf)

    # override values from configuration
    if args.server:
        conf['server'] = args.server
    if args.port:
        conf['port']   = args.port

    expandPaths(conf)
    checkConf(conf, ['server', 'port', 'token', 'proxy'])

    proxyStr = readProxyFile(conf['proxy'])

    proxyCert, _, issuerChains = parse_issuer_cred(proxyStr)

    requestUrl = conf['server'] + ':' + str(conf['port']) + '/proxies'

    try:
        r = requests.post(requestUrl, data={'cert': proxyStr})
    except Exception as e:
        print('error: request: {}'.format(str(e)))
        sys.exit(1)

    if r.status_code != 200:
        print('error: request response: {} - {}'.format(r.status_code, r.text))
        sys.exit(1)

    data = r.json()

    token = data['token']
    csr = x509.load_pem_x509_csr(data['csr'].encode('utf-8'), default_backend())
    cert = x509proxy.sign_request(csr).decode('utf-8')
    chain = proxyCert.public_bytes(serialization.Encoding.PEM).decode('utf-8') + issuerChains + '\n'

    try:
        obj = {'cert': cert, 'chain': chain}
        params = {'token': token}
        r = requests.put(requestUrl, json=obj, params=params)
    except Exception as e:
        print('error: request: {}'.format(str(e)))
        sys.exit(1)

    if r.status_code == 200:
        token = r.json()['token']
        # TODO: exceptions
        if not os.path.exists(conf['token']):
            os.makedirs(os.path.dirname(conf['token']))
        with open(conf['token'], 'w') as f:
            f.write(token)
        os.chmod(conf['token'], 0o600)

        print('Successfully inserted proxy. Access token: {}'.format(token))
    else:
        print('error: request response: {} - {}'.format(r.status_code, r.text))
        sys.exit(1)


if __name__ == '__main__':
    main()
