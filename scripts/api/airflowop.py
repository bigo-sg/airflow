#!/usr/bin/python
import sys
import requests
from lxml import etree
import argparse
import json, os

pwd = os.path.dirname(os.path.realpath(__file__))
with open(pwd + '/../conf/conf.json') as fd:
    config = json.load(fd)
webserver = config['webserver_url']

def upload_dag(user, password, filepath):
    client = requests.session()
    resp = client.get(webserver + '/admin')
    if resp.status_code != 200:
        print("access webserver failed")
        return
    tree = etree.HTML(resp.content)
    csrf = tree.xpath('//input[@name="_csrf_token"]/@value')[0]
    url = webserver+ '/admin/airflow/login'
    data = {
        "username": user,
        "password": password,
        "submit": "Enter",
        "lang": "en",
        "action": "/admin/airflow/login",
        '_csrf_token':csrf
    }
    resp = client.post(url, data=data)
    if resp.status_code != 200:
        print("access webserver failed")
        return

    url = webserver + '/api/experimental/upload_dag'
    files = {'file': open(filepath, 'rb')}
    resp = client.post(url,files=files)
    print(resp, resp.text)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='airflow api')
    sub_args = parser.add_subparsers(help='action', dest='command')
    
    upload_dag_args = sub_args.add_parser('upload_dag', help='upload/update a dag')
    upload_dag_args.add_argument('-u', '--user', type=str, help='user name', required=True)
    upload_dag_args.add_argument('-p', '--password', type=str, help='password', required=True)
    upload_dag_args.add_argument('-f', '--file', type=str, help='dag file', required=True)

    args = parser.parse_args()
    if args.command == 'upload_dag':
        upload_dag(args.user, args.password, args.file)
    


