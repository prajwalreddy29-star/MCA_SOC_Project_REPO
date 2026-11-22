# Databricks notebook source
import re
import json
import requests as r
from requests.auth import HTTPBasicAuth
from typing import Optional, Dict, Any


class CollibraAPIClient:
    def __init__(self,
                 username: str,
                 password: str,
                 base_url: str = "https://collibra.example.com/rest/2.0/") \
            -> None:
        # helper attributes
        self.base_url = base_url.rstrip("/") + "/"
        self._cookie_pattern = re.compile(r"JSESSIONID=([\w|\d|\-]+)")

        # credentials
        self.username = username
        self.password = password
        self.basic_auth = HTTPBasicAuth(username, password)

        # session attributes
        self._access_token = None
        self._session_cookie = None

        self.authenticate()

    @staticmethod
    def _request(method: str, **kwargs) -> r.Response:
        endpoint = getattr(r, method)
        response = endpoint(**kwargs)

        if response.status_code == 200:
            return response
        else:
            raise Exception(response.json())

    def authenticate(self) -> None:
        data = {"username": self.username,
                "password": self.password}

        headers = {"Content-Type": "application/json"}

        response = self._request(method="post",
                                 url=f"{self.base_url}auth/sessions",
                                 headers=headers,
                                 data=json.dumps(data))

        response_body = response.json()
        response_header = response.headers

        self._session_cookie = self._cookie_pattern.search(response_header["Set-Cookie"]).group(0)
        self._access_token = response_body["csrfToken"]

    def get_user_info(self, output_to_console: bool = False) -> Optional[Dict[str, Any]]:
        response = self._request(method="get",
                                 url=f"{self.base_url}auth/sessions/current?include=user",
                                 headers={"Authorization": f"Bearer {self._access_token}",
                                          "Cookie": self._session_cookie}
                                 ).json()

        if output_to_console:
            print(response)
        else:
            return response

    def output_module_export(self, table_view_config: str):
        response = self._request(method="post",
                                 url=f"{self.base_url}outputModule/export/csv?validationEnabled=true",
                                 headers={"Cookie": self._session_cookie},
                                 data=table_view_config,
                                 auth=self.basic_auth
                                 )

        return response.text
