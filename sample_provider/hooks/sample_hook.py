from typing import Any, Callable, Dict, Optional, Union

import requests
import tenacity
from requests.auth import HTTPBasicAuth

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook


class SampleHook(BaseHook):
  
  conn_name_attr = 'sample_conn_id'
  default_conn_name = 'http_default'
  conn_type = 'http'
  hook_name = 'HTTP'

  def __init__(
    self,
    method: str = 'POST',
    sample_conn_id: str = default_conn_name,
    auth_type: Any = HTTPBasicAuth,
  ) -> None:
    super().__init__()
    self.sample_conn_id = sample_conn_id
    self.method = method.upper()
    self.base_url: str = ""
    self.auth_type: Any = auth_type

  def get_conn(self, headers: Optional[Dict[Any, Any]] = None) -> requests.Session:
    
    session = requests.Session()

    if self.sample_conn_id:
      conn = self.get_connection(self.sample_conn_id)

      if conn.host and "://" in conn.host:
        self.base_url = conn.host
      else:
        schema = conn.schema if conn.schema else "http"
        host = conn.host if conn.host else ""
        self.base_url = schema + "://" + host

      if conn.port:
        self.base_url = self.base_url + ":" + str(conn.port)
      if conn.login:
        session.auth = self.auth_type(conn.login, conn.password)
      if conn.extra:
        try:
          session.headers.update(conn.extra_dejson)
        except TypeError:
          self.log.warning(
            'Connection to %s has invalid extra fields.', conn.host)
    
    if headers:
      session.headers.update(headers)

    return session

  def run(
    self,
    endpoint: Optional[str] = None,
    data: Optional[Union[Dict[str, Any], str]] = None,
    headers: Optional[Dict[str, Any]] = None,
    **request_kwargs: Any,
  ) -> Any:
    
    session = self.get_conn(headers)

    if self.base_url and not self.base_url.endswith('/') and endpoint and not endpoint.startswith('/'):
      url = self.base_url + '/' + endpoint
    else:
      url = (self.base_url or '') + (endpoint or '')

    if self.method == 'GET':
      req = requests.Request(
        self.method, url, headers=headers)
    else:
      req = requests.Request(
        self.method, url, data=data, headers=headers)

    prepped_request = session.prepare_request(req)

    self.log.info("Sending '%s' to url: %s", self.method, url)

    try:
      response = session.send(prepped_request)
      return response
    except requests.exceptions.ConnectionError as ex:
      self.log.warning(
        '%s Tenacity will retry to execute the operation', ex)
      raise ex
