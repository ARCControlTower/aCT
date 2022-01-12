import os
import argparse
import sys
import asyncio
import aiohttp
import x509proxy

from config import loadConf, checkConf, expandPaths
from common import readProxyFile, addCommonArgs
from delegate_proxy import parse_issuer_cred
from cryptography import x509
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization


def main():
    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(program())
    except Exception as e:
        print('error: {}'.format(e))
        sys.exit(1)


async def program():
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

    async with aiohttp.ClientSession() as session:
        async with session.post(requestUrl, json={'cert': proxyStr}) as resp:
            json = await resp.json()
            if resp.status != 200:
                print('error: request response: {} - {}'.format(resp.status, json['msg']))
                sys.exit(1)

        token = json['token']
        csr = x509.load_pem_x509_csr(json['csr'].encode('utf-8'), default_backend())
        cert = x509proxy.sign_request(csr).decode('utf-8')
        chain = proxyCert.public_bytes(serialization.Encoding.PEM).decode('utf-8') + issuerChains + '\n'

        json = {'cert': cert, 'chain': chain}
        params = {'token': token}
        async with session.put(requestUrl, json=json, params=params) as resp:
            json = await resp.json()
            if resp.status == 200:
                token = json['token']
                # TODO: exceptions
                if not os.path.exists(conf['token']):
                    os.makedirs(os.path.dirname(conf['token']))
                with open(conf['token'], 'w') as f:
                    f.write(token)
                os.chmod(conf['token'], 0o600)
                print('Successfully inserted proxy. Access token: {}'.format(token))
            else:
                print('error: request response: {} - {}'.format(resp.status, json['msg']))
                sys.exit(1)
