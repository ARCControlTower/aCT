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


# TODO: delete what is remaining on backend if proxy submission fails


async def deleteProxy(session, requestUrl, token):
    headers = {'Authorization': 'Bearer ' + token}
    try:
        async with session.delete(requestUrl, headers=headers) as resp:
            json = await resp.json()
            if resp.status != 204:
                print('response error: deleting proxy: {}'.format(json['msg']))
                return
    except aiohttp.ClientError as e:
        print('HTTP client error: deleting proxy: {}'.format(e))


async def uploadProxy(session, requestUrl, proxyStr, conf):
    # submit proxy cert part to get CSR
    try:
        async with session.post(requestUrl, json={'cert': proxyStr}) as resp:
            json = await resp.json()
            if resp.status != 200:
                print('response error: {} - {}'.format(resp.status, json['msg']))
                sys.exit(1)
    except aiohttp.ClientError as e:
        print('HTTP client error: sending request for CSR: {}'.format(e))
        sys.exit(1)
    token = json['token']

    # sign CSR
    try:
        proxyCert, _, issuerChains = parse_issuer_cred(proxyStr)
        csr = x509.load_pem_x509_csr(json['csr'].encode('utf-8'), default_backend())
        cert = x509proxy.sign_request(csr).decode('utf-8')
        chain = proxyCert.public_bytes(serialization.Encoding.PEM).decode('utf-8') + issuerChains + '\n'
    except Exception as e:
        print('error generating proxy: {}'.format(e))
        await deleteProxy(session, requestUrl, token)
        sys.exit(1)

    # upload signed cert
    json = {'cert': cert, 'chain': chain}
    headers = {'Authorization': 'Bearer ' + token}
    try:
        async with session.put(requestUrl, json=json, headers=headers) as resp:
            json = await resp.json()
            if resp.status != 200:
                print('response error: {} - {}'.format(resp.status, json['msg']))
                await deleteProxy(session, requestUrl, token)
                sys.exit(1)
    except aiohttp.ClientError as e:
        print('HTTP client error: uploading signed proxy: {}'.format(e))
        await deleteProxy(session, requestUrl, token)
        sys.exit(1)

    # store auth token
    token = json['token']
    try:
        if not os.path.exists(conf['token']):
            os.makedirs(os.path.dirname(conf['token']))
        with open(conf['token'], 'w') as f:
            f.write(token)
        os.chmod(conf['token'], 0o600)
    except Exception as e:
        print('error saving token: {}'.format(e))
        await deleteProxy(session, requestUrl, token)
        sys.exit(1)

    print('Successfully inserted proxy. Access token: {}'.format(token))


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

    requestUrl = conf['server'] + ':' + str(conf['port']) + '/proxies'

    async with aiohttp.ClientSession() as session:
        await uploadProxy(session, requestUrl, proxyStr, conf)
