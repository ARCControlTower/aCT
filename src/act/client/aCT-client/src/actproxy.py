import argparse
import os

import httpx
import trio
from cryptography import x509
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

import x509proxy
from common import addCommonArgs, readProxyFile, runWithSIGINTHandler
from config import checkConf, expandPaths, loadConf
from delegate_proxy import parse_issuer_cred

# TODO: use exceptions instead of system exit


async def deleteProxy(client, requestUrl, token):
    headers = {'Authorization': 'Bearer ' + token}
    try:
        resp = await client.delete(requestUrl, headers=headers)
        json = resp.json()
    except httpx.RequestError as e:
        print('request error: {}'.format(e))
        return

    if resp.status_code != 204:
        print('response error: deleting proxy: {} - {}'.format(resp.status_code, json['msg']))


async def uploadProxy(client, requestUrl, proxyStr, conf):
    # submit proxy cert part to get CSR
    try:
        resp = await client.post(requestUrl, json={'cert': proxyStr})
        json = resp.json()
    except (httpx.RequestError, trio.Cancelled) as e:
        print('request error: {}'.format(e))
        return
    if resp.status_code != 200:
        print('response error: {} - {}'.format(resp.status, json['msg']))
        return
    token = json['token']

    # sign CSR
    try:
        proxyCert, _, issuerChains = parse_issuer_cred(proxyStr)
        csr = x509.load_pem_x509_csr(json['csr'].encode('utf-8'), default_backend())
        cert = x509proxy.sign_request(csr).decode('utf-8')
        chain = proxyCert.public_bytes(serialization.Encoding.PEM).decode('utf-8') + issuerChains + '\n'
    except Exception as e:
        print('error generating proxy: {}'.format(e))
        await deleteProxy(client, requestUrl, token)
        return

    # upload signed cert
    json = {'cert': cert, 'chain': chain}
    headers = {'Authorization': 'Bearer ' + token}
    try:
        resp = await client.put(requestUrl, json=json, headers=headers)
        json = resp.json()
    except httpx.RequestError as e:
        print('request error: {}'.format(e))
        await deleteProxy(client, requestUrl, token)
        return
    if resp.status_code != 200:
        print('response error: {} - {}'.format(resp.status_code, json['msg']))
        await deleteProxy(client, requestUrl, token)
        return

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
        await deleteProxy(client, requestUrl, token)
        return

    print('Successfully inserted proxy. Access token: {}'.format(token))


def main():
    trio.run(runWithSIGINTHandler, program)


async def program():
    parser = argparse.ArgumentParser(description='Submit proxy to aCT server')
    addCommonArgs(parser)
    args = parser.parse_args()

    conf = loadConf(path=args.conf)

    # override values from configuration
    if args.server:
        conf['server'] = args.server
    if args.port:
        conf['port'] = args.port

    expandPaths(conf)
    checkConf(conf, ['server', 'port', 'token', 'proxy'])

    proxyStr = readProxyFile(conf['proxy'])

    requestUrl = conf['server'] + ':' + str(conf['port']) + '/proxies'

    with trio.CancelScope(shield=True):
        async with httpx.AsyncClient() as client:
            await uploadProxy(client, requestUrl, proxyStr, conf)
