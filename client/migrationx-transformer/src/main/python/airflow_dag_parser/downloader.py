import logging as log
from pathlib import Path

import requests
from requests import HTTPError


class DagDownloader(object):
    def __init__(self, host, download_dir=None, cookie: dict = None, username=None, password=None):
        self.download_dir = download_dir
        self.host = host
        self.cookie = cookie
        self.username = username
        self.password = password

    def get_dags(self):
        url = f'http://{self.host}/api/v1/dags'
        res = self.do_request_json(url)
        dags = res['dags']
        return dags

    def download_dags(self):
        dags = self.get_dags()
        if self.download_dir:
            Path(self.download_dir).mkdir(parents=True, exist_ok=True)
        count: int = 0
        for dag in dags:
            dag_id = dag['dag_id']
            file_loc = dag['fileloc']
            file_name = file_loc.split('/')[-1]
            file_token = dag['file_token']
            log.info(f"downloading dag {dag_id} with file_loc {file_loc}")
            content = self.download_code(file_token)
            self.write_file(file_name, content)
            count += 1
        log.info(f"finished downloading dags, total: {count}")

    def download_code(self, file_token) -> None:
        url = f'http://{self.host}/api/v1/dagSources/{file_token}'
        res = self.get_request(url)
        if res.status_code != 200:
            raise HTTPError(res.status_code)
        return res.content

    def write_file(self, file_name, content):
        if self.download_dir:
            file = f"{self.download_dir}/{file_name}"
        else:
            file = f"{file_name}"
        with open(file, mode="wb") as file:
            file.write(content)

    def do_request_json(self, url):
        res = self.get_request(url)
        if res.status_code != 200:
            raise HTTPError(res.status_code)
        return res.json()

    def get_request(self, url):
        if self.cookie:
            headers = self.cookie
            res = requests.get(url=url, headers=headers)
        elif self.username and self.password:
            res = requests.get(url=url, auth=(self.username, self.password))
        else:
            res = requests.get(url=url)
        return res

    def download_task(self, dag_id):
        url = f'http://{self.host}/api/v1/dags/{dag_id}/tasks'
        res = self.get_request(url)
        if res.status_code != 200:
            raise HTTPError(res.status_code)
        json = res.json()
        return json
