from typing import Any, Callable, Dict, Optional

from airflow.exceptions import AirflowException
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults

from sample_provider.hooks.sample_hook import SampleHook

class SampleSensor(BaseSensorOperator):
  
  template_fields = [
    'endpoint',
    'request_params',
    'headers'
  ]

  @apply_defaults
  def __init__(
    self,
    *,
    endpoint: str,
    sample_conn_id: str = 'conn_sample',
    method: str = 'GET',
    request_params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, Any]] = None,
    **kwargs: Any,
  ) -> None:
    super().__init__(**kwargs)
    self.endpoint = endpoint
    self.sample_conn_id = sample_conn_id
    self.request_params = request_params or {}
    self.headers = headers or {}
    self.hook = SampleHook(method=method, sample_conn_id=sample_conn_id)

  def poke(self, context: Dict[Any, Any]) -> bool:
    from airflow.utils.operator_helpers import make_kwargs_callable

    self.log.info('Poking: %s', self.endpoint)
    try:
      response = self.hook.run(
        self.endpoint,
        data=self.request_params,
        headers=self.headers,
      )
      if response.status_code == 404:
        return False

    except AirflowException as exc:
      if str(exc).startswith("404"):
        return False

      raise exc

    return True