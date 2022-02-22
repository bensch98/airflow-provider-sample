def get_provider_info():
  return {
    "package-name": "airflow-provider-sample", # required
    "name": "Sample Airflow Provider", # required
    "description": "A sample template for airflow providers.", # required
    # "hook-class-names": ["sample_provider.hooks.sample_hook.SampleHook"],
    "extra-links": ["sample_provider.operators.sample_operator.ExtraLink"],
    "versions": ["0.0.1"] # required
  }
