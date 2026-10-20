# Databricks notebook source


# COMMAND ----------

def _cli_runner_create_uv_wrapper(uv_exec_path, uv_version, env):
  import os

  uv_wrapper_code = f"""#!/bin/bash -e
if [[ ! -f "{uv_exec_path}" ]]; then
  curl -LsSf https://astral.sh/uv/{uv_version}/install.sh | sh
  exec "$HOME/.local/bin/uv" "$@"
fi
exec "{uv_exec_path}" "$@"
"""
  uv_wrapper_path = "uv"
  with open(uv_wrapper_path, "w") as file:
    file.write(uv_wrapper_code)
  os.chmod(uv_wrapper_path, 0o755)
  env["PATH"] = env.get("PATH", "") + os.pathsep + os.getcwd()


def _cli_runner_get_venv_path_from_databricks_yml(cwd):
  import yaml

  databricks_yml_path = f"{cwd}/databricks.yml"
  with open(databricks_yml_path, "r") as f:
    data = yaml.safe_load(f)
    return data.get("python", {}).get("venv_path", ".venv")


def _cli_runner_run_cli_command(cli_command, cwd, tmp_file_path, include_stderr, env, *, cli_upstream):
  import os
  import subprocess

  stderr_arg = subprocess.PIPE if include_stderr else None
  if tmp_file_path:
    os.makedirs(os.path.dirname(tmp_file_path), exist_ok=True)
    result = subprocess.run(cli_command, cwd=cwd, stdout=subprocess.PIPE, stderr=stderr_arg, text=True, env=env)
    with open(tmp_file_path, 'w') as output_file:
        output_file.write(result.stdout)
        print(result.stdout)
  else:
    result = subprocess.run(cli_command, cwd=cwd, stderr=stderr_arg, text=True, env=env)
  if result.returncode != 0:
    error_msg = f"Command {result.args[:3]} failed with exit code {result.returncode}"
    if include_stderr and result.stderr:
      error_msg += f":\n{result.stderr}"
    raise RuntimeError(error_msg)

  if cli_upstream == "genie-code":
    print(result.stderr)

    try:
      with open("/Workspace/.proc/self/status", "r") as status_file:
        status_file.read()
    except Exception as e:
      print(f"Warning: Failed to read /Workspace/.proc/self/status: {e}")


def _cli_runner_run_cli_command_with_token(cli_command, cwd, tmp_file_path, include_stderr, token, env, *, cli_upstream):
  _cli_runner_run_cli_command(
    cli_command,
    cwd,
    tmp_file_path,
    include_stderr,
    {**env, "DATABRICKS_TOKEN": token},
    cli_upstream=cli_upstream,
  )


def _cli_runner_get_download_url(workspace_url, token, file_api_path, runner_download_error_marker):
  import requests
  import time

  api_create_download_url = f"{workspace_url}/api/2.0/fs/create-download-url"
  headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
  data = {"path": file_api_path}

  max_attempts = 3
  last_error = None
  for attempt in range(max_attempts):
    try:
      response = requests.post(api_create_download_url, headers=headers, json=data)
      if response.status_code == 200:
        return response.json()["url"]
      last_error = f"HTTP {response.status_code} {response.text}"
    except requests.RequestException as e:
      last_error = str(e)
    if attempt < max_attempts - 1:
      time.sleep(2 ** attempt)

  print(f"{runner_download_error_marker}: {file_api_path} {last_error}")
  raise RuntimeError(f"Failed to download {file_api_path}: {last_error}")


def _databricks_cli_runner():
  import hashlib
  import os
  import platform
  import requests
  import shlex
  import shutil
  import subprocess
  import tempfile
  import time
  import zipfile
  from dbruntime.databricks_repl_context import get_context

  cli_version = "1.17.0"
  terraform_version = "1.5.5"
  provider_version = "1.132.0"
  uv_version = "0.6.10"
  cwd = "/Repos/jakkularajeswari266@gmail.com/adb_cicd"
  args = "bundle debug list-targets -o json"
  tmp_file = "/Workspace/Users/jakkularajeswari266@gmail.com/.bundle/DABDemo/.output/list-targets-output-1l8grz8ozjb.json"
  dev_cli_snippet = ""
  artifact_download_error_prefix = "DABsRunnerError: Failed to download CLI dependency"
  runner_download_error_marker = "DabsRunnerErrorFallback: Downloading error, using fallback URL"
  allow_python_execution = "false"
  enable_yaml_sync = "false"
  enable_direct_engine = "true"
  force_allow_record_deployment_history = "false"
  include_stderr_in_error = "false"
  cli_upstream = "dabs-in-workspace"
  enable_phase_telemetry = "true"
  disable_uv_wrapper = "true"
  enforce_serverless = "true"

  if enforce_serverless == "true" and os.environ.get("IS_SERVERLESS", "").strip().lower() != "true":
    raise RuntimeError("Databricks CLI runner must run on serverless compute")

  setup_runner_environment_start = time.monotonic()
  ctx = get_context()
  token = ctx.apiToken
  workspace_url = f"https://{spark.conf.get('spark.databricks.workspaceUrl')}"
  cpu_arch = "linux_arm64" if platform.machine() == "aarch64" else "linux_amd64"

  env = os.environ.copy()

  def emit_event(phase, start_time):
    if enable_phase_telemetry == "true":
      print(f"\n__RUNNER_PHASE__:{phase}:{(time.monotonic() - start_time) * 1000:.0f}", flush=True)

  def get_download_url(file_api_path):
    return _cli_runner_get_download_url(
      workspace_url, token, file_api_path, runner_download_error_marker
    )

  def download_and_extract(file_url, dest_dir):
    response = requests.get(file_url)
    if response.status_code != 200:
      raise RuntimeError(f"HTTP {response.status_code} while downloading artifact: {response.text}")
    tmp_file = f"{dest_dir}.zip"

    with open(tmp_file, "wb") as zip_file:
        zip_file.write(response.content)
    with zipfile.ZipFile(tmp_file, 'r') as zip_ref:
        zip_ref.extractall(dest_dir)

  def download_artifact_if_not_exists(input_file_name, binary_name, dest_path, tmp_dir):
    if os.path.exists(dest_path):
      return

    try:
      os.makedirs(os.path.dirname(dest_path), exist_ok=True)

      path_prefix = f"/DatabricksInternal/Jobs/ArtifactRepository/public/databricks-cli-binaries/{cli_version}"
      download_url = get_download_url(f"{path_prefix}/{input_file_name}.zip")

      tmp_archive_dir = f"{tmp_dir}/{input_file_name}"
      download_and_extract(download_url, tmp_archive_dir)

      src_path = f"{tmp_archive_dir}/{binary_name}"
      if not os.path.exists(src_path):
        available = ", ".join(os.listdir(tmp_archive_dir)) if os.path.isdir(tmp_archive_dir) else "(missing archive directory)"
        raise RuntimeError(f"Expected binary '{binary_name}' not found in archive contents: {available}")

      shutil.move(src_path, dest_path)
      os.chmod(dest_path, 0o755)
    except Exception as e:
      error_msg = f"{artifact_download_error_prefix} {input_file_name}/{binary_name} CLI v{cli_version}, TF v{terraform_version}, TF provider v{provider_version}: {str(e)}"
      raise RuntimeError(f"{error_msg}")

  tmp_base = os.path.expanduser("~/tmp")
  if not os.path.exists(tmp_base):
    os.makedirs(tmp_base)
  os.chmod(tmp_base, 0o700)

  with tempfile.TemporaryDirectory(dir=tmp_base) as tmpdirname:
      emit_event("setup_runner_environment", setup_runner_environment_start)

      download_artifacts_start = time.monotonic()
      cli_exec_path = f"{tmp_base}/databricks_{cli_version}_{cpu_arch}"
      download_artifact_if_not_exists(
        f"databricks_cli_{cpu_arch}",
        "databricks",
        cli_exec_path,
        tmpdirname
      )

      terraform_base = f"{tmp_base}/terraform"
      terraform_exec_path = f"{terraform_base}/terraform_{terraform_version}_{cpu_arch}"
      download_artifact_if_not_exists(
        f"terraform_{cpu_arch}",
        "terraform",
        terraform_exec_path,
        tmpdirname
      )

      provider_mirror_base = f"{terraform_base}/providers"
      provider_mirror_dir = f"{provider_mirror_base}/registry.terraform.io/databricks/databricks/{provider_version}/{cpu_arch}"
      os.makedirs(provider_mirror_dir, exist_ok=True)
      provider_exec_path = f"{provider_mirror_dir}/terraform-provider-databricks_v{provider_version}"
      download_artifact_if_not_exists(
        f"terraform_provider_{cpu_arch}",
        f"terraform-provider-databricks_v{provider_version}",
        provider_exec_path,
        tmpdirname
      )

      if disable_uv_wrapper != "true":
        uv_base = f"{tmp_base}/uv"
        uv_exec_path = f"{uv_base}/uv_{uv_version}_{cpu_arch}"
        try:
            download_artifact_if_not_exists(
              f"uv_{cpu_arch}",
              "uv",
              uv_exec_path,
              tmpdirname
            )
        except Exception:
            pass
      else:
        uv_exec_path = "uv"
      emit_event("download_artifacts", download_artifacts_start)

      setup_cli_environment_start = time.monotonic()
      if disable_uv_wrapper != "true":
        _cli_runner_create_uv_wrapper(uv_exec_path, uv_version, env)

      terraform_cli_config_path = f"{terraform_base}/terraform-cli-config"
      if not os.path.exists(terraform_cli_config_path):
        terraform_cli_config = f'''provider_installation {{
  filesystem_mirror {{
    path    = "{provider_mirror_base}"
    include = ["registry.terraform.io/databricks/databricks"]
  }}
  direct {{
    exclude = ["registry.terraform.io/databricks/databricks"]
  }}
}}
disable_checkpoint = true
disable_checkpoint_signature = true
'''

        with open(terraform_cli_config_path, 'w') as f:
          f.write(terraform_cli_config)

      cwd = os.path.normpath(f"/Workspace{cwd}")

      env["DATABRICKS_TF_VERSION"] = terraform_version
      env["DATABRICKS_TF_EXEC_PATH"] = terraform_exec_path
      env["DATABRICKS_TF_PROVIDER_VERSION"] = provider_version
      env["DATABRICKS_TF_CLI_CONFIG_FILE"] = terraform_cli_config_path
      env["TF_CLI_CONFIG_FILE"] = terraform_cli_config_path

      if dev_cli_snippet:
        cli_path = cli_exec_path
        exec(dev_cli_snippet, globals(), locals())

      env["DATABRICKS_BUNDLE_TMP"] = f"{tmp_base}/bundle-2-{hashlib.sha256(cwd.encode()).hexdigest()}"
      env["DATABRICKS_CLI_UPSTREAM"] = cli_upstream

      if enable_yaml_sync == "true":
        env["DATABRICKS_BUNDLE_ENABLE_EXPERIMENTAL_YAML_SYNC"] = "1"
      else:
        env.pop("DATABRICKS_BUNDLE_ENABLE_EXPERIMENTAL_YAML_SYNC", None)

      if enable_direct_engine == "true":
        env["DATABRICKS_BUNDLE_ENGINE"] = "direct"
      else:
        env.pop("DATABRICKS_BUNDLE_ENGINE", None)

      if force_allow_record_deployment_history == "true":
        env["DATABRICKS_BUNDLE_FORCE_ALLOW_RECORD_DEPLOYMENT_HISTORY"] = "true"
        env["DATABRICKS_BUNDLE_INIT_DEPLOYMENT_HISTORY"] = "true"
      else:
        env.pop("DATABRICKS_BUNDLE_FORCE_ALLOW_RECORD_DEPLOYMENT_HISTORY", None)
        env.pop("DATABRICKS_BUNDLE_INIT_DEPLOYMENT_HISTORY", None)

      emit_event("setup_cli_environment", setup_cli_environment_start)

      if allow_python_execution == "true":
        install_dependencies_start = time.monotonic()
        venv_path = _cli_runner_get_venv_path_from_databricks_yml(cwd)
        venv_folder_path = os.path.normpath(f"{cwd}/{venv_path}")
        venv_bin_path = f"{venv_folder_path}/bin"

        os.makedirs(venv_bin_path, exist_ok=True)

        python3_wrapper_path = f"{venv_bin_path}/python3"
        if not os.path.exists(python3_wrapper_path):
          with open(python3_wrapper_path, "w") as f:
            f.write('#!/usr/bin/env bash\nexec python3 "$@"\n')
          os.chmod(python3_wrapper_path, 0o755)

        pyvenv_cfg_path = f"{venv_folder_path}/pyvenv.cfg"
        if not os.path.exists(pyvenv_cfg_path):
          with open(pyvenv_cfg_path, "w") as f:
            pass

        pyproject_path = f"{cwd}/pyproject.toml"
        uv_pip_install_args = [uv_exec_path, "pip", "install", "--python", python3_wrapper_path, "--link-mode=copy"]

        override_file = None
        if os.path.exists(pyproject_path):
          uv_pip_install_args.extend(["-r", pyproject_path, "--group", "dev"])
        else:
          uv_pip_install_args.append(f"databricks-bundles=={cli_version}")

        try:
          subprocess.run(uv_pip_install_args, check=True, cwd=cwd, stdout=subprocess.DEVNULL, env=env)
        finally:
          if override_file and os.path.exists(override_file):
            os.remove(override_file)
        emit_event("install_dependencies", install_dependencies_start)

        env.pop("DATABRICKS_BUNDLE_RESTRICTED_CODE_EXECUTION", None)
      else:
        env["DATABRICKS_BUNDLE_RESTRICTED_CODE_EXECUTION"] = "1"

      env.pop("DATABRICKS_CLUSTER_ID", None)

      with tempfile.NamedTemporaryFile(mode='w+', dir=tmpdirname, delete=True) as databrickscfg_file:
          profile_name = "workspace_profile"
          databrickscfg_file.write(f"[workspace_profile]\nhost = {workspace_url}")
          databrickscfg_file.flush()
          env["DATABRICKS_CONFIG_FILE"] = databrickscfg_file.name

          cli_command = [cli_exec_path] + shlex.split(args) + ["-p", profile_name]
          tmp_file_path = tmp_file

          run_cli_start = time.monotonic()
          run_cli_phase = "run_cli_genie" if cli_upstream == "genie-code" else "run_cli"
          try:
            _cli_runner_run_cli_command_with_token(
              cli_command,
              cwd,
              tmp_file_path,
              include_stderr_in_error == "true",
              token,
              env,
              cli_upstream=cli_upstream,
            )
          finally:
            emit_event(run_cli_phase, run_cli_start)


try:
  _databricks_cli_runner()
finally:
  del (
    _databricks_cli_runner,
    _cli_runner_create_uv_wrapper,
    _cli_runner_get_venv_path_from_databricks_yml,
    _cli_runner_get_download_url,
    _cli_runner_run_cli_command,
    _cli_runner_run_cli_command_with_token,
  )
None


# COMMAND ----------

def _cli_runner_create_uv_wrapper(uv_exec_path, uv_version, env):
  import os

  uv_wrapper_code = f"""#!/bin/bash -e
if [[ ! -f "{uv_exec_path}" ]]; then
  curl -LsSf https://astral.sh/uv/{uv_version}/install.sh | sh
  exec "$HOME/.local/bin/uv" "$@"
fi
exec "{uv_exec_path}" "$@"
"""
  uv_wrapper_path = "uv"
  with open(uv_wrapper_path, "w") as file:
    file.write(uv_wrapper_code)
  os.chmod(uv_wrapper_path, 0o755)
  env["PATH"] = env.get("PATH", "") + os.pathsep + os.getcwd()


def _cli_runner_get_venv_path_from_databricks_yml(cwd):
  import yaml

  databricks_yml_path = f"{cwd}/databricks.yml"
  with open(databricks_yml_path, "r") as f:
    data = yaml.safe_load(f)
    return data.get("python", {}).get("venv_path", ".venv")


def _cli_runner_run_cli_command(cli_command, cwd, tmp_file_path, include_stderr, env, *, cli_upstream):
  import os
  import subprocess

  stderr_arg = subprocess.PIPE if include_stderr else None
  if tmp_file_path:
    os.makedirs(os.path.dirname(tmp_file_path), exist_ok=True)
    result = subprocess.run(cli_command, cwd=cwd, stdout=subprocess.PIPE, stderr=stderr_arg, text=True, env=env)
    with open(tmp_file_path, 'w') as output_file:
        output_file.write(result.stdout)
        print(result.stdout)
  else:
    result = subprocess.run(cli_command, cwd=cwd, stderr=stderr_arg, text=True, env=env)
  if result.returncode != 0:
    error_msg = f"Command {result.args[:3]} failed with exit code {result.returncode}"
    if include_stderr and result.stderr:
      error_msg += f":\n{result.stderr}"
    raise RuntimeError(error_msg)

  if cli_upstream == "genie-code":
    print(result.stderr)

    try:
      with open("/Workspace/.proc/self/status", "r") as status_file:
        status_file.read()
    except Exception as e:
      print(f"Warning: Failed to read /Workspace/.proc/self/status: {e}")


def _cli_runner_run_cli_command_with_token(cli_command, cwd, tmp_file_path, include_stderr, token, env, *, cli_upstream):
  _cli_runner_run_cli_command(
    cli_command,
    cwd,
    tmp_file_path,
    include_stderr,
    {**env, "DATABRICKS_TOKEN": token},
    cli_upstream=cli_upstream,
  )


def _cli_runner_get_download_url(workspace_url, token, file_api_path, runner_download_error_marker):
  import requests
  import time

  api_create_download_url = f"{workspace_url}/api/2.0/fs/create-download-url"
  headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
  data = {"path": file_api_path}

  max_attempts = 3
  last_error = None
  for attempt in range(max_attempts):
    try:
      response = requests.post(api_create_download_url, headers=headers, json=data)
      if response.status_code == 200:
        return response.json()["url"]
      last_error = f"HTTP {response.status_code} {response.text}"
    except requests.RequestException as e:
      last_error = str(e)
    if attempt < max_attempts - 1:
      time.sleep(2 ** attempt)

  print(f"{runner_download_error_marker}: {file_api_path} {last_error}")
  raise RuntimeError(f"Failed to download {file_api_path}: {last_error}")


def _databricks_cli_runner():
  import hashlib
  import os
  import platform
  import requests
  import shlex
  import shutil
  import subprocess
  import tempfile
  import time
  import zipfile
  from dbruntime.databricks_repl_context import get_context

  cli_version = "1.17.0"
  terraform_version = "1.5.5"
  provider_version = "1.132.0"
  uv_version = "0.6.10"
  cwd = "/Repos/jakkularajeswari266@gmail.com/adb_cicd"
  args = "bundle debug list-targets -o json"
  tmp_file = "/Workspace/Users/jakkularajeswari266@gmail.com/.bundle/DABDemo/.output/list-targets-output-y3biatc4k4.json"
  dev_cli_snippet = ""
  artifact_download_error_prefix = "DABsRunnerError: Failed to download CLI dependency"
  runner_download_error_marker = "DabsRunnerErrorFallback: Downloading error, using fallback URL"
  allow_python_execution = "false"
  enable_yaml_sync = "true"
  enable_direct_engine = "true"
  force_allow_record_deployment_history = "false"
  include_stderr_in_error = "false"
  cli_upstream = "dabs-in-workspace"
  enable_phase_telemetry = "true"
  disable_uv_wrapper = "true"
  enforce_serverless = "true"

  if enforce_serverless == "true" and os.environ.get("IS_SERVERLESS", "").strip().lower() != "true":
    raise RuntimeError("Databricks CLI runner must run on serverless compute")

  setup_runner_environment_start = time.monotonic()
  ctx = get_context()
  token = ctx.apiToken
  workspace_url = f"https://{spark.conf.get('spark.databricks.workspaceUrl')}"
  cpu_arch = "linux_arm64" if platform.machine() == "aarch64" else "linux_amd64"

  env = os.environ.copy()

  def emit_event(phase, start_time):
    if enable_phase_telemetry == "true":
      print(f"\n__RUNNER_PHASE__:{phase}:{(time.monotonic() - start_time) * 1000:.0f}", flush=True)

  def get_download_url(file_api_path):
    return _cli_runner_get_download_url(
      workspace_url, token, file_api_path, runner_download_error_marker
    )

  def download_and_extract(file_url, dest_dir):
    response = requests.get(file_url)
    if response.status_code != 200:
      raise RuntimeError(f"HTTP {response.status_code} while downloading artifact: {response.text}")
    tmp_file = f"{dest_dir}.zip"

    with open(tmp_file, "wb") as zip_file:
        zip_file.write(response.content)
    with zipfile.ZipFile(tmp_file, 'r') as zip_ref:
        zip_ref.extractall(dest_dir)

  def download_artifact_if_not_exists(input_file_name, binary_name, dest_path, tmp_dir):
    if os.path.exists(dest_path):
      return

    try:
      os.makedirs(os.path.dirname(dest_path), exist_ok=True)

      path_prefix = f"/DatabricksInternal/Jobs/ArtifactRepository/public/databricks-cli-binaries/{cli_version}"
      download_url = get_download_url(f"{path_prefix}/{input_file_name}.zip")

      tmp_archive_dir = f"{tmp_dir}/{input_file_name}"
      download_and_extract(download_url, tmp_archive_dir)

      src_path = f"{tmp_archive_dir}/{binary_name}"
      if not os.path.exists(src_path):
        available = ", ".join(os.listdir(tmp_archive_dir)) if os.path.isdir(tmp_archive_dir) else "(missing archive directory)"
        raise RuntimeError(f"Expected binary '{binary_name}' not found in archive contents: {available}")

      shutil.move(src_path, dest_path)
      os.chmod(dest_path, 0o755)
    except Exception as e:
      error_msg = f"{artifact_download_error_prefix} {input_file_name}/{binary_name} CLI v{cli_version}, TF v{terraform_version}, TF provider v{provider_version}: {str(e)}"
      raise RuntimeError(f"{error_msg}")

  tmp_base = os.path.expanduser("~/tmp")
  if not os.path.exists(tmp_base):
    os.makedirs(tmp_base)
  os.chmod(tmp_base, 0o700)

  with tempfile.TemporaryDirectory(dir=tmp_base) as tmpdirname:
      emit_event("setup_runner_environment", setup_runner_environment_start)

      download_artifacts_start = time.monotonic()
      cli_exec_path = f"{tmp_base}/databricks_{cli_version}_{cpu_arch}"
      download_artifact_if_not_exists(
        f"databricks_cli_{cpu_arch}",
        "databricks",
        cli_exec_path,
        tmpdirname
      )

      terraform_base = f"{tmp_base}/terraform"
      terraform_exec_path = f"{terraform_base}/terraform_{terraform_version}_{cpu_arch}"
      download_artifact_if_not_exists(
        f"terraform_{cpu_arch}",
        "terraform",
        terraform_exec_path,
        tmpdirname
      )

      provider_mirror_base = f"{terraform_base}/providers"
      provider_mirror_dir = f"{provider_mirror_base}/registry.terraform.io/databricks/databricks/{provider_version}/{cpu_arch}"
      os.makedirs(provider_mirror_dir, exist_ok=True)
      provider_exec_path = f"{provider_mirror_dir}/terraform-provider-databricks_v{provider_version}"
      download_artifact_if_not_exists(
        f"terraform_provider_{cpu_arch}",
        f"terraform-provider-databricks_v{provider_version}",
        provider_exec_path,
        tmpdirname
      )

      if disable_uv_wrapper != "true":
        uv_base = f"{tmp_base}/uv"
        uv_exec_path = f"{uv_base}/uv_{uv_version}_{cpu_arch}"
        try:
            download_artifact_if_not_exists(
              f"uv_{cpu_arch}",
              "uv",
              uv_exec_path,
              tmpdirname
            )
        except Exception:
            pass
      else:
        uv_exec_path = "uv"
      emit_event("download_artifacts", download_artifacts_start)

      setup_cli_environment_start = time.monotonic()
      if disable_uv_wrapper != "true":
        _cli_runner_create_uv_wrapper(uv_exec_path, uv_version, env)

      terraform_cli_config_path = f"{terraform_base}/terraform-cli-config"
      if not os.path.exists(terraform_cli_config_path):
        terraform_cli_config = f'''provider_installation {{
  filesystem_mirror {{
    path    = "{provider_mirror_base}"
    include = ["registry.terraform.io/databricks/databricks"]
  }}
  direct {{
    exclude = ["registry.terraform.io/databricks/databricks"]
  }}
}}
disable_checkpoint = true
disable_checkpoint_signature = true
'''

        with open(terraform_cli_config_path, 'w') as f:
          f.write(terraform_cli_config)

      cwd = os.path.normpath(f"/Workspace{cwd}")

      env["DATABRICKS_TF_VERSION"] = terraform_version
      env["DATABRICKS_TF_EXEC_PATH"] = terraform_exec_path
      env["DATABRICKS_TF_PROVIDER_VERSION"] = provider_version
      env["DATABRICKS_TF_CLI_CONFIG_FILE"] = terraform_cli_config_path
      env["TF_CLI_CONFIG_FILE"] = terraform_cli_config_path

      if dev_cli_snippet:
        cli_path = cli_exec_path
        exec(dev_cli_snippet, globals(), locals())

      env["DATABRICKS_BUNDLE_TMP"] = f"{tmp_base}/bundle-2-{hashlib.sha256(cwd.encode()).hexdigest()}"
      env["DATABRICKS_CLI_UPSTREAM"] = cli_upstream

      if enable_yaml_sync == "true":
        env["DATABRICKS_BUNDLE_ENABLE_EXPERIMENTAL_YAML_SYNC"] = "1"
      else:
        env.pop("DATABRICKS_BUNDLE_ENABLE_EXPERIMENTAL_YAML_SYNC", None)

      if enable_direct_engine == "true":
        env["DATABRICKS_BUNDLE_ENGINE"] = "direct"
      else:
        env.pop("DATABRICKS_BUNDLE_ENGINE", None)

      if force_allow_record_deployment_history == "true":
        env["DATABRICKS_BUNDLE_FORCE_ALLOW_RECORD_DEPLOYMENT_HISTORY"] = "true"
        env["DATABRICKS_BUNDLE_INIT_DEPLOYMENT_HISTORY"] = "true"
      else:
        env.pop("DATABRICKS_BUNDLE_FORCE_ALLOW_RECORD_DEPLOYMENT_HISTORY", None)
        env.pop("DATABRICKS_BUNDLE_INIT_DEPLOYMENT_HISTORY", None)

      emit_event("setup_cli_environment", setup_cli_environment_start)

      if allow_python_execution == "true":
        install_dependencies_start = time.monotonic()
        venv_path = _cli_runner_get_venv_path_from_databricks_yml(cwd)
        venv_folder_path = os.path.normpath(f"{cwd}/{venv_path}")
        venv_bin_path = f"{venv_folder_path}/bin"

        os.makedirs(venv_bin_path, exist_ok=True)

        python3_wrapper_path = f"{venv_bin_path}/python3"
        if not os.path.exists(python3_wrapper_path):
          with open(python3_wrapper_path, "w") as f:
            f.write('#!/usr/bin/env bash\nexec python3 "$@"\n')
          os.chmod(python3_wrapper_path, 0o755)

        pyvenv_cfg_path = f"{venv_folder_path}/pyvenv.cfg"
        if not os.path.exists(pyvenv_cfg_path):
          with open(pyvenv_cfg_path, "w") as f:
            pass

        pyproject_path = f"{cwd}/pyproject.toml"
        uv_pip_install_args = [uv_exec_path, "pip", "install", "--python", python3_wrapper_path, "--link-mode=copy"]

        override_file = None
        if os.path.exists(pyproject_path):
          uv_pip_install_args.extend(["-r", pyproject_path, "--group", "dev"])
        else:
          uv_pip_install_args.append(f"databricks-bundles=={cli_version}")

        try:
          subprocess.run(uv_pip_install_args, check=True, cwd=cwd, stdout=subprocess.DEVNULL, env=env)
        finally:
          if override_file and os.path.exists(override_file):
            os.remove(override_file)
        emit_event("install_dependencies", install_dependencies_start)

        env.pop("DATABRICKS_BUNDLE_RESTRICTED_CODE_EXECUTION", None)
      else:
        env["DATABRICKS_BUNDLE_RESTRICTED_CODE_EXECUTION"] = "1"

      env.pop("DATABRICKS_CLUSTER_ID", None)

      with tempfile.NamedTemporaryFile(mode='w+', dir=tmpdirname, delete=True) as databrickscfg_file:
          profile_name = "workspace_profile"
          databrickscfg_file.write(f"[workspace_profile]\nhost = {workspace_url}")
          databrickscfg_file.flush()
          env["DATABRICKS_CONFIG_FILE"] = databrickscfg_file.name

          cli_command = [cli_exec_path] + shlex.split(args) + ["-p", profile_name]
          tmp_file_path = tmp_file

          run_cli_start = time.monotonic()
          run_cli_phase = "run_cli_genie" if cli_upstream == "genie-code" else "run_cli"
          try:
            _cli_runner_run_cli_command_with_token(
              cli_command,
              cwd,
              tmp_file_path,
              include_stderr_in_error == "true",
              token,
              env,
              cli_upstream=cli_upstream,
            )
          finally:
            emit_event(run_cli_phase, run_cli_start)


try:
  _databricks_cli_runner()
finally:
  del (
    _databricks_cli_runner,
    _cli_runner_create_uv_wrapper,
    _cli_runner_get_venv_path_from_databricks_yml,
    _cli_runner_get_download_url,
    _cli_runner_run_cli_command,
    _cli_runner_run_cli_command_with_token,
  )
None


# COMMAND ----------

def _cli_runner_create_uv_wrapper(uv_exec_path, uv_version, env):
  import os

  uv_wrapper_code = f"""#!/bin/bash -e
if [[ ! -f "{uv_exec_path}" ]]; then
  curl -LsSf https://astral.sh/uv/{uv_version}/install.sh | sh
  exec "$HOME/.local/bin/uv" "$@"
fi
exec "{uv_exec_path}" "$@"
"""
  uv_wrapper_path = "uv"
  with open(uv_wrapper_path, "w") as file:
    file.write(uv_wrapper_code)
  os.chmod(uv_wrapper_path, 0o755)
  env["PATH"] = env.get("PATH", "") + os.pathsep + os.getcwd()


def _cli_runner_get_venv_path_from_databricks_yml(cwd):
  import yaml

  databricks_yml_path = f"{cwd}/databricks.yml"
  with open(databricks_yml_path, "r") as f:
    data = yaml.safe_load(f)
    return data.get("python", {}).get("venv_path", ".venv")


def _cli_runner_run_cli_command(cli_command, cwd, tmp_file_path, include_stderr, env, *, cli_upstream):
  import os
  import subprocess

  stderr_arg = subprocess.PIPE if include_stderr else None
  if tmp_file_path:
    os.makedirs(os.path.dirname(tmp_file_path), exist_ok=True)
    result = subprocess.run(cli_command, cwd=cwd, stdout=subprocess.PIPE, stderr=stderr_arg, text=True, env=env)
    with open(tmp_file_path, 'w') as output_file:
        output_file.write(result.stdout)
        print(result.stdout)
  else:
    result = subprocess.run(cli_command, cwd=cwd, stderr=stderr_arg, text=True, env=env)
  if result.returncode != 0:
    error_msg = f"Command {result.args[:3]} failed with exit code {result.returncode}"
    if include_stderr and result.stderr:
      error_msg += f":\n{result.stderr}"
    raise RuntimeError(error_msg)

  if cli_upstream == "genie-code":
    print(result.stderr)

    try:
      with open("/Workspace/.proc/self/status", "r") as status_file:
        status_file.read()
    except Exception as e:
      print(f"Warning: Failed to read /Workspace/.proc/self/status: {e}")


def _cli_runner_run_cli_command_with_token(cli_command, cwd, tmp_file_path, include_stderr, token, env, *, cli_upstream):
  _cli_runner_run_cli_command(
    cli_command,
    cwd,
    tmp_file_path,
    include_stderr,
    {**env, "DATABRICKS_TOKEN": token},
    cli_upstream=cli_upstream,
  )


def _cli_runner_get_download_url(workspace_url, token, file_api_path, runner_download_error_marker):
  import requests
  import time

  api_create_download_url = f"{workspace_url}/api/2.0/fs/create-download-url"
  headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
  data = {"path": file_api_path}

  max_attempts = 3
  last_error = None
  for attempt in range(max_attempts):
    try:
      response = requests.post(api_create_download_url, headers=headers, json=data)
      if response.status_code == 200:
        return response.json()["url"]
      last_error = f"HTTP {response.status_code} {response.text}"
    except requests.RequestException as e:
      last_error = str(e)
    if attempt < max_attempts - 1:
      time.sleep(2 ** attempt)

  print(f"{runner_download_error_marker}: {file_api_path} {last_error}")
  raise RuntimeError(f"Failed to download {file_api_path}: {last_error}")


def _databricks_cli_runner():
  import hashlib
  import os
  import platform
  import requests
  import shlex
  import shutil
  import subprocess
  import tempfile
  import time
  import zipfile
  from dbruntime.databricks_repl_context import get_context

  cli_version = "1.17.0"
  terraform_version = "1.5.5"
  provider_version = "1.132.0"
  uv_version = "0.6.10"
  cwd = "/Repos/jakkularajeswari266@gmail.com/adb_cicd"
  args = "bundle summary -o json --include-locations --target dev"
  tmp_file = "/Workspace/Users/jakkularajeswari266@gmail.com/.bundle/DABDemo/.output/summary-output-5828sbt16q3.json"
  dev_cli_snippet = ""
  artifact_download_error_prefix = "DABsRunnerError: Failed to download CLI dependency"
  runner_download_error_marker = "DabsRunnerErrorFallback: Downloading error, using fallback URL"
  allow_python_execution = "false"
  enable_yaml_sync = "true"
  enable_direct_engine = "true"
  force_allow_record_deployment_history = "false"
  include_stderr_in_error = "false"
  cli_upstream = "dabs-in-workspace"
  enable_phase_telemetry = "true"
  disable_uv_wrapper = "true"
  enforce_serverless = "true"

  if enforce_serverless == "true" and os.environ.get("IS_SERVERLESS", "").strip().lower() != "true":
    raise RuntimeError("Databricks CLI runner must run on serverless compute")

  setup_runner_environment_start = time.monotonic()
  ctx = get_context()
  token = ctx.apiToken
  workspace_url = f"https://{spark.conf.get('spark.databricks.workspaceUrl')}"
  cpu_arch = "linux_arm64" if platform.machine() == "aarch64" else "linux_amd64"

  env = os.environ.copy()

  def emit_event(phase, start_time):
    if enable_phase_telemetry == "true":
      print(f"\n__RUNNER_PHASE__:{phase}:{(time.monotonic() - start_time) * 1000:.0f}", flush=True)

  def get_download_url(file_api_path):
    return _cli_runner_get_download_url(
      workspace_url, token, file_api_path, runner_download_error_marker
    )

  def download_and_extract(file_url, dest_dir):
    response = requests.get(file_url)
    if response.status_code != 200:
      raise RuntimeError(f"HTTP {response.status_code} while downloading artifact: {response.text}")
    tmp_file = f"{dest_dir}.zip"

    with open(tmp_file, "wb") as zip_file:
        zip_file.write(response.content)
    with zipfile.ZipFile(tmp_file, 'r') as zip_ref:
        zip_ref.extractall(dest_dir)

  def download_artifact_if_not_exists(input_file_name, binary_name, dest_path, tmp_dir):
    if os.path.exists(dest_path):
      return

    try:
      os.makedirs(os.path.dirname(dest_path), exist_ok=True)

      path_prefix = f"/DatabricksInternal/Jobs/ArtifactRepository/public/databricks-cli-binaries/{cli_version}"
      download_url = get_download_url(f"{path_prefix}/{input_file_name}.zip")

      tmp_archive_dir = f"{tmp_dir}/{input_file_name}"
      download_and_extract(download_url, tmp_archive_dir)

      src_path = f"{tmp_archive_dir}/{binary_name}"
      if not os.path.exists(src_path):
        available = ", ".join(os.listdir(tmp_archive_dir)) if os.path.isdir(tmp_archive_dir) else "(missing archive directory)"
        raise RuntimeError(f"Expected binary '{binary_name}' not found in archive contents: {available}")

      shutil.move(src_path, dest_path)
      os.chmod(dest_path, 0o755)
    except Exception as e:
      error_msg = f"{artifact_download_error_prefix} {input_file_name}/{binary_name} CLI v{cli_version}, TF v{terraform_version}, TF provider v{provider_version}: {str(e)}"
      raise RuntimeError(f"{error_msg}")

  tmp_base = os.path.expanduser("~/tmp")
  if not os.path.exists(tmp_base):
    os.makedirs(tmp_base)
  os.chmod(tmp_base, 0o700)

  with tempfile.TemporaryDirectory(dir=tmp_base) as tmpdirname:
      emit_event("setup_runner_environment", setup_runner_environment_start)

      download_artifacts_start = time.monotonic()
      cli_exec_path = f"{tmp_base}/databricks_{cli_version}_{cpu_arch}"
      download_artifact_if_not_exists(
        f"databricks_cli_{cpu_arch}",
        "databricks",
        cli_exec_path,
        tmpdirname
      )

      terraform_base = f"{tmp_base}/terraform"
      terraform_exec_path = f"{terraform_base}/terraform_{terraform_version}_{cpu_arch}"
      download_artifact_if_not_exists(
        f"terraform_{cpu_arch}",
        "terraform",
        terraform_exec_path,
        tmpdirname
      )

      provider_mirror_base = f"{terraform_base}/providers"
      provider_mirror_dir = f"{provider_mirror_base}/registry.terraform.io/databricks/databricks/{provider_version}/{cpu_arch}"
      os.makedirs(provider_mirror_dir, exist_ok=True)
      provider_exec_path = f"{provider_mirror_dir}/terraform-provider-databricks_v{provider_version}"
      download_artifact_if_not_exists(
        f"terraform_provider_{cpu_arch}",
        f"terraform-provider-databricks_v{provider_version}",
        provider_exec_path,
        tmpdirname
      )

      if disable_uv_wrapper != "true":
        uv_base = f"{tmp_base}/uv"
        uv_exec_path = f"{uv_base}/uv_{uv_version}_{cpu_arch}"
        try:
            download_artifact_if_not_exists(
              f"uv_{cpu_arch}",
              "uv",
              uv_exec_path,
              tmpdirname
            )
        except Exception:
            pass
      else:
        uv_exec_path = "uv"
      emit_event("download_artifacts", download_artifacts_start)

      setup_cli_environment_start = time.monotonic()
      if disable_uv_wrapper != "true":
        _cli_runner_create_uv_wrapper(uv_exec_path, uv_version, env)

      terraform_cli_config_path = f"{terraform_base}/terraform-cli-config"
      if not os.path.exists(terraform_cli_config_path):
        terraform_cli_config = f'''provider_installation {{
  filesystem_mirror {{
    path    = "{provider_mirror_base}"
    include = ["registry.terraform.io/databricks/databricks"]
  }}
  direct {{
    exclude = ["registry.terraform.io/databricks/databricks"]
  }}
}}
disable_checkpoint = true
disable_checkpoint_signature = true
'''

        with open(terraform_cli_config_path, 'w') as f:
          f.write(terraform_cli_config)

      cwd = os.path.normpath(f"/Workspace{cwd}")

      env["DATABRICKS_TF_VERSION"] = terraform_version
      env["DATABRICKS_TF_EXEC_PATH"] = terraform_exec_path
      env["DATABRICKS_TF_PROVIDER_VERSION"] = provider_version
      env["DATABRICKS_TF_CLI_CONFIG_FILE"] = terraform_cli_config_path
      env["TF_CLI_CONFIG_FILE"] = terraform_cli_config_path

      if dev_cli_snippet:
        cli_path = cli_exec_path
        exec(dev_cli_snippet, globals(), locals())

      env["DATABRICKS_BUNDLE_TMP"] = f"{tmp_base}/bundle-2-{hashlib.sha256(cwd.encode()).hexdigest()}"
      env["DATABRICKS_CLI_UPSTREAM"] = cli_upstream

      if enable_yaml_sync == "true":
        env["DATABRICKS_BUNDLE_ENABLE_EXPERIMENTAL_YAML_SYNC"] = "1"
      else:
        env.pop("DATABRICKS_BUNDLE_ENABLE_EXPERIMENTAL_YAML_SYNC", None)

      if enable_direct_engine == "true":
        env["DATABRICKS_BUNDLE_ENGINE"] = "direct"
      else:
        env.pop("DATABRICKS_BUNDLE_ENGINE", None)

      if force_allow_record_deployment_history == "true":
        env["DATABRICKS_BUNDLE_FORCE_ALLOW_RECORD_DEPLOYMENT_HISTORY"] = "true"
        env["DATABRICKS_BUNDLE_INIT_DEPLOYMENT_HISTORY"] = "true"
      else:
        env.pop("DATABRICKS_BUNDLE_FORCE_ALLOW_RECORD_DEPLOYMENT_HISTORY", None)
        env.pop("DATABRICKS_BUNDLE_INIT_DEPLOYMENT_HISTORY", None)

      emit_event("setup_cli_environment", setup_cli_environment_start)

      if allow_python_execution == "true":
        install_dependencies_start = time.monotonic()
        venv_path = _cli_runner_get_venv_path_from_databricks_yml(cwd)
        venv_folder_path = os.path.normpath(f"{cwd}/{venv_path}")
        venv_bin_path = f"{venv_folder_path}/bin"

        os.makedirs(venv_bin_path, exist_ok=True)

        python3_wrapper_path = f"{venv_bin_path}/python3"
        if not os.path.exists(python3_wrapper_path):
          with open(python3_wrapper_path, "w") as f:
            f.write('#!/usr/bin/env bash\nexec python3 "$@"\n')
          os.chmod(python3_wrapper_path, 0o755)

        pyvenv_cfg_path = f"{venv_folder_path}/pyvenv.cfg"
        if not os.path.exists(pyvenv_cfg_path):
          with open(pyvenv_cfg_path, "w") as f:
            pass

        pyproject_path = f"{cwd}/pyproject.toml"
        uv_pip_install_args = [uv_exec_path, "pip", "install", "--python", python3_wrapper_path, "--link-mode=copy"]

        override_file = None
        if os.path.exists(pyproject_path):
          uv_pip_install_args.extend(["-r", pyproject_path, "--group", "dev"])
        else:
          uv_pip_install_args.append(f"databricks-bundles=={cli_version}")

        try:
          subprocess.run(uv_pip_install_args, check=True, cwd=cwd, stdout=subprocess.DEVNULL, env=env)
        finally:
          if override_file and os.path.exists(override_file):
            os.remove(override_file)
        emit_event("install_dependencies", install_dependencies_start)

        env.pop("DATABRICKS_BUNDLE_RESTRICTED_CODE_EXECUTION", None)
      else:
        env["DATABRICKS_BUNDLE_RESTRICTED_CODE_EXECUTION"] = "1"

      env.pop("DATABRICKS_CLUSTER_ID", None)

      with tempfile.NamedTemporaryFile(mode='w+', dir=tmpdirname, delete=True) as databrickscfg_file:
          profile_name = "workspace_profile"
          databrickscfg_file.write(f"[workspace_profile]\nhost = {workspace_url}")
          databrickscfg_file.flush()
          env["DATABRICKS_CONFIG_FILE"] = databrickscfg_file.name

          cli_command = [cli_exec_path] + shlex.split(args) + ["-p", profile_name]
          tmp_file_path = tmp_file

          run_cli_start = time.monotonic()
          run_cli_phase = "run_cli_genie" if cli_upstream == "genie-code" else "run_cli"
          try:
            _cli_runner_run_cli_command_with_token(
              cli_command,
              cwd,
              tmp_file_path,
              include_stderr_in_error == "true",
              token,
              env,
              cli_upstream=cli_upstream,
            )
          finally:
            emit_event(run_cli_phase, run_cli_start)


try:
  _databricks_cli_runner()
finally:
  del (
    _databricks_cli_runner,
    _cli_runner_create_uv_wrapper,
    _cli_runner_get_venv_path_from_databricks_yml,
    _cli_runner_get_download_url,
    _cli_runner_run_cli_command,
    _cli_runner_run_cli_command_with_token,
  )
None


# COMMAND ----------

def _cli_runner_create_uv_wrapper(uv_exec_path, uv_version, env):
  import os

  uv_wrapper_code = f"""#!/bin/bash -e
if [[ ! -f "{uv_exec_path}" ]]; then
  curl -LsSf https://astral.sh/uv/{uv_version}/install.sh | sh
  exec "$HOME/.local/bin/uv" "$@"
fi
exec "{uv_exec_path}" "$@"
"""
  uv_wrapper_path = "uv"
  with open(uv_wrapper_path, "w") as file:
    file.write(uv_wrapper_code)
  os.chmod(uv_wrapper_path, 0o755)
  env["PATH"] = env.get("PATH", "") + os.pathsep + os.getcwd()


def _cli_runner_get_venv_path_from_databricks_yml(cwd):
  import yaml

  databricks_yml_path = f"{cwd}/databricks.yml"
  with open(databricks_yml_path, "r") as f:
    data = yaml.safe_load(f)
    return data.get("python", {}).get("venv_path", ".venv")


def _cli_runner_run_cli_command(cli_command, cwd, tmp_file_path, include_stderr, env, *, cli_upstream):
  import os
  import subprocess

  stderr_arg = subprocess.PIPE if include_stderr else None
  if tmp_file_path:
    os.makedirs(os.path.dirname(tmp_file_path), exist_ok=True)
    result = subprocess.run(cli_command, cwd=cwd, stdout=subprocess.PIPE, stderr=stderr_arg, text=True, env=env)
    with open(tmp_file_path, 'w') as output_file:
        output_file.write(result.stdout)
        print(result.stdout)
  else:
    result = subprocess.run(cli_command, cwd=cwd, stderr=stderr_arg, text=True, env=env)
  if result.returncode != 0:
    error_msg = f"Command {result.args[:3]} failed with exit code {result.returncode}"
    if include_stderr and result.stderr:
      error_msg += f":\n{result.stderr}"
    raise RuntimeError(error_msg)

  if cli_upstream == "genie-code":
    print(result.stderr)

    try:
      with open("/Workspace/.proc/self/status", "r") as status_file:
        status_file.read()
    except Exception as e:
      print(f"Warning: Failed to read /Workspace/.proc/self/status: {e}")


def _cli_runner_run_cli_command_with_token(cli_command, cwd, tmp_file_path, include_stderr, token, env, *, cli_upstream):
  _cli_runner_run_cli_command(
    cli_command,
    cwd,
    tmp_file_path,
    include_stderr,
    {**env, "DATABRICKS_TOKEN": token},
    cli_upstream=cli_upstream,
  )


def _cli_runner_get_download_url(workspace_url, token, file_api_path, runner_download_error_marker):
  import requests
  import time

  api_create_download_url = f"{workspace_url}/api/2.0/fs/create-download-url"
  headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
  data = {"path": file_api_path}

  max_attempts = 3
  last_error = None
  for attempt in range(max_attempts):
    try:
      response = requests.post(api_create_download_url, headers=headers, json=data)
      if response.status_code == 200:
        return response.json()["url"]
      last_error = f"HTTP {response.status_code} {response.text}"
    except requests.RequestException as e:
      last_error = str(e)
    if attempt < max_attempts - 1:
      time.sleep(2 ** attempt)

  print(f"{runner_download_error_marker}: {file_api_path} {last_error}")
  raise RuntimeError(f"Failed to download {file_api_path}: {last_error}")


def _databricks_cli_runner():
  import hashlib
  import os
  import platform
  import requests
  import shlex
  import shutil
  import subprocess
  import tempfile
  import time
  import zipfile
  from dbruntime.databricks_repl_context import get_context

  cli_version = "1.17.0"
  terraform_version = "1.5.5"
  provider_version = "1.132.0"
  uv_version = "0.6.10"
  cwd = "/Repos/jakkularajeswari266@gmail.com/adb_cicd"
  args = "bundle debug list-targets -o json"
  tmp_file = "/Workspace/Users/jakkularajeswari266@gmail.com/.bundle/DABDemo/.output/list-targets-output-nkr6kdhnhi.json"
  dev_cli_snippet = ""
  artifact_download_error_prefix = "DABsRunnerError: Failed to download CLI dependency"
  runner_download_error_marker = "DabsRunnerErrorFallback: Downloading error, using fallback URL"
  allow_python_execution = "false"
  enable_yaml_sync = "true"
  enable_direct_engine = "true"
  force_allow_record_deployment_history = "false"
  include_stderr_in_error = "false"
  cli_upstream = "dabs-in-workspace"
  enable_phase_telemetry = "true"
  disable_uv_wrapper = "true"
  enforce_serverless = "true"

  if enforce_serverless == "true" and os.environ.get("IS_SERVERLESS", "").strip().lower() != "true":
    raise RuntimeError("Databricks CLI runner must run on serverless compute")

  setup_runner_environment_start = time.monotonic()
  ctx = get_context()
  token = ctx.apiToken
  workspace_url = f"https://{spark.conf.get('spark.databricks.workspaceUrl')}"
  cpu_arch = "linux_arm64" if platform.machine() == "aarch64" else "linux_amd64"

  env = os.environ.copy()

  def emit_event(phase, start_time):
    if enable_phase_telemetry == "true":
      print(f"\n__RUNNER_PHASE__:{phase}:{(time.monotonic() - start_time) * 1000:.0f}", flush=True)

  def get_download_url(file_api_path):
    return _cli_runner_get_download_url(
      workspace_url, token, file_api_path, runner_download_error_marker
    )

  def download_and_extract(file_url, dest_dir):
    response = requests.get(file_url)
    if response.status_code != 200:
      raise RuntimeError(f"HTTP {response.status_code} while downloading artifact: {response.text}")
    tmp_file = f"{dest_dir}.zip"

    with open(tmp_file, "wb") as zip_file:
        zip_file.write(response.content)
    with zipfile.ZipFile(tmp_file, 'r') as zip_ref:
        zip_ref.extractall(dest_dir)

  def download_artifact_if_not_exists(input_file_name, binary_name, dest_path, tmp_dir):
    if os.path.exists(dest_path):
      return

    try:
      os.makedirs(os.path.dirname(dest_path), exist_ok=True)

      path_prefix = f"/DatabricksInternal/Jobs/ArtifactRepository/public/databricks-cli-binaries/{cli_version}"
      download_url = get_download_url(f"{path_prefix}/{input_file_name}.zip")

      tmp_archive_dir = f"{tmp_dir}/{input_file_name}"
      download_and_extract(download_url, tmp_archive_dir)

      src_path = f"{tmp_archive_dir}/{binary_name}"
      if not os.path.exists(src_path):
        available = ", ".join(os.listdir(tmp_archive_dir)) if os.path.isdir(tmp_archive_dir) else "(missing archive directory)"
        raise RuntimeError(f"Expected binary '{binary_name}' not found in archive contents: {available}")

      shutil.move(src_path, dest_path)
      os.chmod(dest_path, 0o755)
    except Exception as e:
      error_msg = f"{artifact_download_error_prefix} {input_file_name}/{binary_name} CLI v{cli_version}, TF v{terraform_version}, TF provider v{provider_version}: {str(e)}"
      raise RuntimeError(f"{error_msg}")

  tmp_base = os.path.expanduser("~/tmp")
  if not os.path.exists(tmp_base):
    os.makedirs(tmp_base)
  os.chmod(tmp_base, 0o700)

  with tempfile.TemporaryDirectory(dir=tmp_base) as tmpdirname:
      emit_event("setup_runner_environment", setup_runner_environment_start)

      download_artifacts_start = time.monotonic()
      cli_exec_path = f"{tmp_base}/databricks_{cli_version}_{cpu_arch}"
      download_artifact_if_not_exists(
        f"databricks_cli_{cpu_arch}",
        "databricks",
        cli_exec_path,
        tmpdirname
      )

      terraform_base = f"{tmp_base}/terraform"
      terraform_exec_path = f"{terraform_base}/terraform_{terraform_version}_{cpu_arch}"
      download_artifact_if_not_exists(
        f"terraform_{cpu_arch}",
        "terraform",
        terraform_exec_path,
        tmpdirname
      )

      provider_mirror_base = f"{terraform_base}/providers"
      provider_mirror_dir = f"{provider_mirror_base}/registry.terraform.io/databricks/databricks/{provider_version}/{cpu_arch}"
      os.makedirs(provider_mirror_dir, exist_ok=True)
      provider_exec_path = f"{provider_mirror_dir}/terraform-provider-databricks_v{provider_version}"
      download_artifact_if_not_exists(
        f"terraform_provider_{cpu_arch}",
        f"terraform-provider-databricks_v{provider_version}",
        provider_exec_path,
        tmpdirname
      )

      if disable_uv_wrapper != "true":
        uv_base = f"{tmp_base}/uv"
        uv_exec_path = f"{uv_base}/uv_{uv_version}_{cpu_arch}"
        try:
            download_artifact_if_not_exists(
              f"uv_{cpu_arch}",
              "uv",
              uv_exec_path,
              tmpdirname
            )
        except Exception:
            pass
      else:
        uv_exec_path = "uv"
      emit_event("download_artifacts", download_artifacts_start)

      setup_cli_environment_start = time.monotonic()
      if disable_uv_wrapper != "true":
        _cli_runner_create_uv_wrapper(uv_exec_path, uv_version, env)

      terraform_cli_config_path = f"{terraform_base}/terraform-cli-config"
      if not os.path.exists(terraform_cli_config_path):
        terraform_cli_config = f'''provider_installation {{
  filesystem_mirror {{
    path    = "{provider_mirror_base}"
    include = ["registry.terraform.io/databricks/databricks"]
  }}
  direct {{
    exclude = ["registry.terraform.io/databricks/databricks"]
  }}
}}
disable_checkpoint = true
disable_checkpoint_signature = true
'''

        with open(terraform_cli_config_path, 'w') as f:
          f.write(terraform_cli_config)

      cwd = os.path.normpath(f"/Workspace{cwd}")

      env["DATABRICKS_TF_VERSION"] = terraform_version
      env["DATABRICKS_TF_EXEC_PATH"] = terraform_exec_path
      env["DATABRICKS_TF_PROVIDER_VERSION"] = provider_version
      env["DATABRICKS_TF_CLI_CONFIG_FILE"] = terraform_cli_config_path
      env["TF_CLI_CONFIG_FILE"] = terraform_cli_config_path

      if dev_cli_snippet:
        cli_path = cli_exec_path
        exec(dev_cli_snippet, globals(), locals())

      env["DATABRICKS_BUNDLE_TMP"] = f"{tmp_base}/bundle-2-{hashlib.sha256(cwd.encode()).hexdigest()}"
      env["DATABRICKS_CLI_UPSTREAM"] = cli_upstream

      if enable_yaml_sync == "true":
        env["DATABRICKS_BUNDLE_ENABLE_EXPERIMENTAL_YAML_SYNC"] = "1"
      else:
        env.pop("DATABRICKS_BUNDLE_ENABLE_EXPERIMENTAL_YAML_SYNC", None)

      if enable_direct_engine == "true":
        env["DATABRICKS_BUNDLE_ENGINE"] = "direct"
      else:
        env.pop("DATABRICKS_BUNDLE_ENGINE", None)

      if force_allow_record_deployment_history == "true":
        env["DATABRICKS_BUNDLE_FORCE_ALLOW_RECORD_DEPLOYMENT_HISTORY"] = "true"
        env["DATABRICKS_BUNDLE_INIT_DEPLOYMENT_HISTORY"] = "true"
      else:
        env.pop("DATABRICKS_BUNDLE_FORCE_ALLOW_RECORD_DEPLOYMENT_HISTORY", None)
        env.pop("DATABRICKS_BUNDLE_INIT_DEPLOYMENT_HISTORY", None)

      emit_event("setup_cli_environment", setup_cli_environment_start)

      if allow_python_execution == "true":
        install_dependencies_start = time.monotonic()
        venv_path = _cli_runner_get_venv_path_from_databricks_yml(cwd)
        venv_folder_path = os.path.normpath(f"{cwd}/{venv_path}")
        venv_bin_path = f"{venv_folder_path}/bin"

        os.makedirs(venv_bin_path, exist_ok=True)

        python3_wrapper_path = f"{venv_bin_path}/python3"
        if not os.path.exists(python3_wrapper_path):
          with open(python3_wrapper_path, "w") as f:
            f.write('#!/usr/bin/env bash\nexec python3 "$@"\n')
          os.chmod(python3_wrapper_path, 0o755)

        pyvenv_cfg_path = f"{venv_folder_path}/pyvenv.cfg"
        if not os.path.exists(pyvenv_cfg_path):
          with open(pyvenv_cfg_path, "w") as f:
            pass

        pyproject_path = f"{cwd}/pyproject.toml"
        uv_pip_install_args = [uv_exec_path, "pip", "install", "--python", python3_wrapper_path, "--link-mode=copy"]

        override_file = None
        if os.path.exists(pyproject_path):
          uv_pip_install_args.extend(["-r", pyproject_path, "--group", "dev"])
        else:
          uv_pip_install_args.append(f"databricks-bundles=={cli_version}")

        try:
          subprocess.run(uv_pip_install_args, check=True, cwd=cwd, stdout=subprocess.DEVNULL, env=env)
        finally:
          if override_file and os.path.exists(override_file):
            os.remove(override_file)
        emit_event("install_dependencies", install_dependencies_start)

        env.pop("DATABRICKS_BUNDLE_RESTRICTED_CODE_EXECUTION", None)
      else:
        env["DATABRICKS_BUNDLE_RESTRICTED_CODE_EXECUTION"] = "1"

      env.pop("DATABRICKS_CLUSTER_ID", None)

      with tempfile.NamedTemporaryFile(mode='w+', dir=tmpdirname, delete=True) as databrickscfg_file:
          profile_name = "workspace_profile"
          databrickscfg_file.write(f"[workspace_profile]\nhost = {workspace_url}")
          databrickscfg_file.flush()
          env["DATABRICKS_CONFIG_FILE"] = databrickscfg_file.name

          cli_command = [cli_exec_path] + shlex.split(args) + ["-p", profile_name]
          tmp_file_path = tmp_file

          run_cli_start = time.monotonic()
          run_cli_phase = "run_cli_genie" if cli_upstream == "genie-code" else "run_cli"
          try:
            _cli_runner_run_cli_command_with_token(
              cli_command,
              cwd,
              tmp_file_path,
              include_stderr_in_error == "true",
              token,
              env,
              cli_upstream=cli_upstream,
            )
          finally:
            emit_event(run_cli_phase, run_cli_start)


try:
  _databricks_cli_runner()
finally:
  del (
    _databricks_cli_runner,
    _cli_runner_create_uv_wrapper,
    _cli_runner_get_venv_path_from_databricks_yml,
    _cli_runner_get_download_url,
    _cli_runner_run_cli_command,
    _cli_runner_run_cli_command_with_token,
  )
None


# COMMAND ----------

def _cli_runner_create_uv_wrapper(uv_exec_path, uv_version, env):
  import os

  uv_wrapper_code = f"""#!/bin/bash -e
if [[ ! -f "{uv_exec_path}" ]]; then
  curl -LsSf https://astral.sh/uv/{uv_version}/install.sh | sh
  exec "$HOME/.local/bin/uv" "$@"
fi
exec "{uv_exec_path}" "$@"
"""
  uv_wrapper_path = "uv"
  with open(uv_wrapper_path, "w") as file:
    file.write(uv_wrapper_code)
  os.chmod(uv_wrapper_path, 0o755)
  env["PATH"] = env.get("PATH", "") + os.pathsep + os.getcwd()


def _cli_runner_get_venv_path_from_databricks_yml(cwd):
  import yaml

  databricks_yml_path = f"{cwd}/databricks.yml"
  with open(databricks_yml_path, "r") as f:
    data = yaml.safe_load(f)
    return data.get("python", {}).get("venv_path", ".venv")


def _cli_runner_run_cli_command(cli_command, cwd, tmp_file_path, include_stderr, env, *, cli_upstream):
  import os
  import subprocess

  stderr_arg = subprocess.PIPE if include_stderr else None
  if tmp_file_path:
    os.makedirs(os.path.dirname(tmp_file_path), exist_ok=True)
    result = subprocess.run(cli_command, cwd=cwd, stdout=subprocess.PIPE, stderr=stderr_arg, text=True, env=env)
    with open(tmp_file_path, 'w') as output_file:
        output_file.write(result.stdout)
        print(result.stdout)
  else:
    result = subprocess.run(cli_command, cwd=cwd, stderr=stderr_arg, text=True, env=env)
  if result.returncode != 0:
    error_msg = f"Command {result.args[:3]} failed with exit code {result.returncode}"
    if include_stderr and result.stderr:
      error_msg += f":\n{result.stderr}"
    raise RuntimeError(error_msg)

  if cli_upstream == "genie-code":
    print(result.stderr)

    try:
      with open("/Workspace/.proc/self/status", "r") as status_file:
        status_file.read()
    except Exception as e:
      print(f"Warning: Failed to read /Workspace/.proc/self/status: {e}")


def _cli_runner_run_cli_command_with_token(cli_command, cwd, tmp_file_path, include_stderr, token, env, *, cli_upstream):
  _cli_runner_run_cli_command(
    cli_command,
    cwd,
    tmp_file_path,
    include_stderr,
    {**env, "DATABRICKS_TOKEN": token},
    cli_upstream=cli_upstream,
  )


def _cli_runner_get_download_url(workspace_url, token, file_api_path, runner_download_error_marker):
  import requests
  import time

  api_create_download_url = f"{workspace_url}/api/2.0/fs/create-download-url"
  headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
  data = {"path": file_api_path}

  max_attempts = 3
  last_error = None
  for attempt in range(max_attempts):
    try:
      response = requests.post(api_create_download_url, headers=headers, json=data)
      if response.status_code == 200:
        return response.json()["url"]
      last_error = f"HTTP {response.status_code} {response.text}"
    except requests.RequestException as e:
      last_error = str(e)
    if attempt < max_attempts - 1:
      time.sleep(2 ** attempt)

  print(f"{runner_download_error_marker}: {file_api_path} {last_error}")
  raise RuntimeError(f"Failed to download {file_api_path}: {last_error}")


def _databricks_cli_runner():
  import hashlib
  import os
  import platform
  import requests
  import shlex
  import shutil
  import subprocess
  import tempfile
  import time
  import zipfile
  from dbruntime.databricks_repl_context import get_context

  cli_version = "1.17.0"
  terraform_version = "1.5.5"
  provider_version = "1.132.0"
  uv_version = "0.6.10"
  cwd = "/Repos/jakkularajeswari266@gmail.com/adb_cicd"
  args = "bundle summary -o json --include-locations --target dev"
  tmp_file = "/Workspace/Users/jakkularajeswari266@gmail.com/.bundle/DABDemo/.output/summary-output-gk8fy7pcsoi.json"
  dev_cli_snippet = ""
  artifact_download_error_prefix = "DABsRunnerError: Failed to download CLI dependency"
  runner_download_error_marker = "DabsRunnerErrorFallback: Downloading error, using fallback URL"
  allow_python_execution = "false"
  enable_yaml_sync = "true"
  enable_direct_engine = "true"
  force_allow_record_deployment_history = "false"
  include_stderr_in_error = "false"
  cli_upstream = "dabs-in-workspace"
  enable_phase_telemetry = "true"
  disable_uv_wrapper = "true"
  enforce_serverless = "true"

  if enforce_serverless == "true" and os.environ.get("IS_SERVERLESS", "").strip().lower() != "true":
    raise RuntimeError("Databricks CLI runner must run on serverless compute")

  setup_runner_environment_start = time.monotonic()
  ctx = get_context()
  token = ctx.apiToken
  workspace_url = f"https://{spark.conf.get('spark.databricks.workspaceUrl')}"
  cpu_arch = "linux_arm64" if platform.machine() == "aarch64" else "linux_amd64"

  env = os.environ.copy()

  def emit_event(phase, start_time):
    if enable_phase_telemetry == "true":
      print(f"\n__RUNNER_PHASE__:{phase}:{(time.monotonic() - start_time) * 1000:.0f}", flush=True)

  def get_download_url(file_api_path):
    return _cli_runner_get_download_url(
      workspace_url, token, file_api_path, runner_download_error_marker
    )

  def download_and_extract(file_url, dest_dir):
    response = requests.get(file_url)
    if response.status_code != 200:
      raise RuntimeError(f"HTTP {response.status_code} while downloading artifact: {response.text}")
    tmp_file = f"{dest_dir}.zip"

    with open(tmp_file, "wb") as zip_file:
        zip_file.write(response.content)
    with zipfile.ZipFile(tmp_file, 'r') as zip_ref:
        zip_ref.extractall(dest_dir)

  def download_artifact_if_not_exists(input_file_name, binary_name, dest_path, tmp_dir):
    if os.path.exists(dest_path):
      return

    try:
      os.makedirs(os.path.dirname(dest_path), exist_ok=True)

      path_prefix = f"/DatabricksInternal/Jobs/ArtifactRepository/public/databricks-cli-binaries/{cli_version}"
      download_url = get_download_url(f"{path_prefix}/{input_file_name}.zip")

      tmp_archive_dir = f"{tmp_dir}/{input_file_name}"
      download_and_extract(download_url, tmp_archive_dir)

      src_path = f"{tmp_archive_dir}/{binary_name}"
      if not os.path.exists(src_path):
        available = ", ".join(os.listdir(tmp_archive_dir)) if os.path.isdir(tmp_archive_dir) else "(missing archive directory)"
        raise RuntimeError(f"Expected binary '{binary_name}' not found in archive contents: {available}")

      shutil.move(src_path, dest_path)
      os.chmod(dest_path, 0o755)
    except Exception as e:
      error_msg = f"{artifact_download_error_prefix} {input_file_name}/{binary_name} CLI v{cli_version}, TF v{terraform_version}, TF provider v{provider_version}: {str(e)}"
      raise RuntimeError(f"{error_msg}")

  tmp_base = os.path.expanduser("~/tmp")
  if not os.path.exists(tmp_base):
    os.makedirs(tmp_base)
  os.chmod(tmp_base, 0o700)

  with tempfile.TemporaryDirectory(dir=tmp_base) as tmpdirname:
      emit_event("setup_runner_environment", setup_runner_environment_start)

      download_artifacts_start = time.monotonic()
      cli_exec_path = f"{tmp_base}/databricks_{cli_version}_{cpu_arch}"
      download_artifact_if_not_exists(
        f"databricks_cli_{cpu_arch}",
        "databricks",
        cli_exec_path,
        tmpdirname
      )

      terraform_base = f"{tmp_base}/terraform"
      terraform_exec_path = f"{terraform_base}/terraform_{terraform_version}_{cpu_arch}"
      download_artifact_if_not_exists(
        f"terraform_{cpu_arch}",
        "terraform",
        terraform_exec_path,
        tmpdirname
      )

      provider_mirror_base = f"{terraform_base}/providers"
      provider_mirror_dir = f"{provider_mirror_base}/registry.terraform.io/databricks/databricks/{provider_version}/{cpu_arch}"
      os.makedirs(provider_mirror_dir, exist_ok=True)
      provider_exec_path = f"{provider_mirror_dir}/terraform-provider-databricks_v{provider_version}"
      download_artifact_if_not_exists(
        f"terraform_provider_{cpu_arch}",
        f"terraform-provider-databricks_v{provider_version}",
        provider_exec_path,
        tmpdirname
      )

      if disable_uv_wrapper != "true":
        uv_base = f"{tmp_base}/uv"
        uv_exec_path = f"{uv_base}/uv_{uv_version}_{cpu_arch}"
        try:
            download_artifact_if_not_exists(
              f"uv_{cpu_arch}",
              "uv",
              uv_exec_path,
              tmpdirname
            )
        except Exception:
            pass
      else:
        uv_exec_path = "uv"
      emit_event("download_artifacts", download_artifacts_start)

      setup_cli_environment_start = time.monotonic()
      if disable_uv_wrapper != "true":
        _cli_runner_create_uv_wrapper(uv_exec_path, uv_version, env)

      terraform_cli_config_path = f"{terraform_base}/terraform-cli-config"
      if not os.path.exists(terraform_cli_config_path):
        terraform_cli_config = f'''provider_installation {{
  filesystem_mirror {{
    path    = "{provider_mirror_base}"
    include = ["registry.terraform.io/databricks/databricks"]
  }}
  direct {{
    exclude = ["registry.terraform.io/databricks/databricks"]
  }}
}}
disable_checkpoint = true
disable_checkpoint_signature = true
'''

        with open(terraform_cli_config_path, 'w') as f:
          f.write(terraform_cli_config)

      cwd = os.path.normpath(f"/Workspace{cwd}")

      env["DATABRICKS_TF_VERSION"] = terraform_version
      env["DATABRICKS_TF_EXEC_PATH"] = terraform_exec_path
      env["DATABRICKS_TF_PROVIDER_VERSION"] = provider_version
      env["DATABRICKS_TF_CLI_CONFIG_FILE"] = terraform_cli_config_path
      env["TF_CLI_CONFIG_FILE"] = terraform_cli_config_path

      if dev_cli_snippet:
        cli_path = cli_exec_path
        exec(dev_cli_snippet, globals(), locals())

      env["DATABRICKS_BUNDLE_TMP"] = f"{tmp_base}/bundle-2-{hashlib.sha256(cwd.encode()).hexdigest()}"
      env["DATABRICKS_CLI_UPSTREAM"] = cli_upstream

      if enable_yaml_sync == "true":
        env["DATABRICKS_BUNDLE_ENABLE_EXPERIMENTAL_YAML_SYNC"] = "1"
      else:
        env.pop("DATABRICKS_BUNDLE_ENABLE_EXPERIMENTAL_YAML_SYNC", None)

      if enable_direct_engine == "true":
        env["DATABRICKS_BUNDLE_ENGINE"] = "direct"
      else:
        env.pop("DATABRICKS_BUNDLE_ENGINE", None)

      if force_allow_record_deployment_history == "true":
        env["DATABRICKS_BUNDLE_FORCE_ALLOW_RECORD_DEPLOYMENT_HISTORY"] = "true"
        env["DATABRICKS_BUNDLE_INIT_DEPLOYMENT_HISTORY"] = "true"
      else:
        env.pop("DATABRICKS_BUNDLE_FORCE_ALLOW_RECORD_DEPLOYMENT_HISTORY", None)
        env.pop("DATABRICKS_BUNDLE_INIT_DEPLOYMENT_HISTORY", None)

      emit_event("setup_cli_environment", setup_cli_environment_start)

      if allow_python_execution == "true":
        install_dependencies_start = time.monotonic()
        venv_path = _cli_runner_get_venv_path_from_databricks_yml(cwd)
        venv_folder_path = os.path.normpath(f"{cwd}/{venv_path}")
        venv_bin_path = f"{venv_folder_path}/bin"

        os.makedirs(venv_bin_path, exist_ok=True)

        python3_wrapper_path = f"{venv_bin_path}/python3"
        if not os.path.exists(python3_wrapper_path):
          with open(python3_wrapper_path, "w") as f:
            f.write('#!/usr/bin/env bash\nexec python3 "$@"\n')
          os.chmod(python3_wrapper_path, 0o755)

        pyvenv_cfg_path = f"{venv_folder_path}/pyvenv.cfg"
        if not os.path.exists(pyvenv_cfg_path):
          with open(pyvenv_cfg_path, "w") as f:
            pass

        pyproject_path = f"{cwd}/pyproject.toml"
        uv_pip_install_args = [uv_exec_path, "pip", "install", "--python", python3_wrapper_path, "--link-mode=copy"]

        override_file = None
        if os.path.exists(pyproject_path):
          uv_pip_install_args.extend(["-r", pyproject_path, "--group", "dev"])
        else:
          uv_pip_install_args.append(f"databricks-bundles=={cli_version}")

        try:
          subprocess.run(uv_pip_install_args, check=True, cwd=cwd, stdout=subprocess.DEVNULL, env=env)
        finally:
          if override_file and os.path.exists(override_file):
            os.remove(override_file)
        emit_event("install_dependencies", install_dependencies_start)

        env.pop("DATABRICKS_BUNDLE_RESTRICTED_CODE_EXECUTION", None)
      else:
        env["DATABRICKS_BUNDLE_RESTRICTED_CODE_EXECUTION"] = "1"

      env.pop("DATABRICKS_CLUSTER_ID", None)

      with tempfile.NamedTemporaryFile(mode='w+', dir=tmpdirname, delete=True) as databrickscfg_file:
          profile_name = "workspace_profile"
          databrickscfg_file.write(f"[workspace_profile]\nhost = {workspace_url}")
          databrickscfg_file.flush()
          env["DATABRICKS_CONFIG_FILE"] = databrickscfg_file.name

          cli_command = [cli_exec_path] + shlex.split(args) + ["-p", profile_name]
          tmp_file_path = tmp_file

          run_cli_start = time.monotonic()
          run_cli_phase = "run_cli_genie" if cli_upstream == "genie-code" else "run_cli"
          try:
            _cli_runner_run_cli_command_with_token(
              cli_command,
              cwd,
              tmp_file_path,
              include_stderr_in_error == "true",
              token,
              env,
              cli_upstream=cli_upstream,
            )
          finally:
            emit_event(run_cli_phase, run_cli_start)


try:
  _databricks_cli_runner()
finally:
  del (
    _databricks_cli_runner,
    _cli_runner_create_uv_wrapper,
    _cli_runner_get_venv_path_from_databricks_yml,
    _cli_runner_get_download_url,
    _cli_runner_run_cli_command,
    _cli_runner_run_cli_command_with_token,
  )
None


# COMMAND ----------

def _cli_runner_create_uv_wrapper(uv_exec_path, uv_version, env):
  import os

  uv_wrapper_code = f"""#!/bin/bash -e
if [[ ! -f "{uv_exec_path}" ]]; then
  curl -LsSf https://astral.sh/uv/{uv_version}/install.sh | sh
  exec "$HOME/.local/bin/uv" "$@"
fi
exec "{uv_exec_path}" "$@"
"""
  uv_wrapper_path = "uv"
  with open(uv_wrapper_path, "w") as file:
    file.write(uv_wrapper_code)
  os.chmod(uv_wrapper_path, 0o755)
  env["PATH"] = env.get("PATH", "") + os.pathsep + os.getcwd()


def _cli_runner_get_venv_path_from_databricks_yml(cwd):
  import yaml

  databricks_yml_path = f"{cwd}/databricks.yml"
  with open(databricks_yml_path, "r") as f:
    data = yaml.safe_load(f)
    return data.get("python", {}).get("venv_path", ".venv")


def _cli_runner_run_cli_command(cli_command, cwd, tmp_file_path, include_stderr, env, *, cli_upstream):
  import os
  import subprocess

  stderr_arg = subprocess.PIPE if include_stderr else None
  if tmp_file_path:
    os.makedirs(os.path.dirname(tmp_file_path), exist_ok=True)
    result = subprocess.run(cli_command, cwd=cwd, stdout=subprocess.PIPE, stderr=stderr_arg, text=True, env=env)
    with open(tmp_file_path, 'w') as output_file:
        output_file.write(result.stdout)
        print(result.stdout)
  else:
    result = subprocess.run(cli_command, cwd=cwd, stderr=stderr_arg, text=True, env=env)
  if result.returncode != 0:
    error_msg = f"Command {result.args[:3]} failed with exit code {result.returncode}"
    if include_stderr and result.stderr:
      error_msg += f":\n{result.stderr}"
    raise RuntimeError(error_msg)

  if cli_upstream == "genie-code":
    print(result.stderr)

    try:
      with open("/Workspace/.proc/self/status", "r") as status_file:
        status_file.read()
    except Exception as e:
      print(f"Warning: Failed to read /Workspace/.proc/self/status: {e}")


def _cli_runner_run_cli_command_with_token(cli_command, cwd, tmp_file_path, include_stderr, token, env, *, cli_upstream):
  _cli_runner_run_cli_command(
    cli_command,
    cwd,
    tmp_file_path,
    include_stderr,
    {**env, "DATABRICKS_TOKEN": token},
    cli_upstream=cli_upstream,
  )


def _cli_runner_get_download_url(workspace_url, token, file_api_path, runner_download_error_marker):
  import requests
  import time

  api_create_download_url = f"{workspace_url}/api/2.0/fs/create-download-url"
  headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
  data = {"path": file_api_path}

  max_attempts = 3
  last_error = None
  for attempt in range(max_attempts):
    try:
      response = requests.post(api_create_download_url, headers=headers, json=data)
      if response.status_code == 200:
        return response.json()["url"]
      last_error = f"HTTP {response.status_code} {response.text}"
    except requests.RequestException as e:
      last_error = str(e)
    if attempt < max_attempts - 1:
      time.sleep(2 ** attempt)

  print(f"{runner_download_error_marker}: {file_api_path} {last_error}")
  raise RuntimeError(f"Failed to download {file_api_path}: {last_error}")


def _databricks_cli_runner():
  import hashlib
  import os
  import platform
  import requests
  import shlex
  import shutil
  import subprocess
  import tempfile
  import time
  import zipfile
  from dbruntime.databricks_repl_context import get_context

  cli_version = "1.17.0"
  terraform_version = "1.5.5"
  provider_version = "1.132.0"
  uv_version = "0.6.10"
  cwd = "/Repos/jakkularajeswari266@gmail.com/adb_cicd"
  args = "bundle summary -o json --include-locations --target dev"
  tmp_file = "/Workspace/Users/jakkularajeswari266@gmail.com/.bundle/DABDemo/.output/summary-output-ctvjblid7tn.json"
  dev_cli_snippet = ""
  artifact_download_error_prefix = "DABsRunnerError: Failed to download CLI dependency"
  runner_download_error_marker = "DabsRunnerErrorFallback: Downloading error, using fallback URL"
  allow_python_execution = "false"
  enable_yaml_sync = "true"
  enable_direct_engine = "true"
  force_allow_record_deployment_history = "false"
  include_stderr_in_error = "false"
  cli_upstream = "dabs-in-workspace"
  enable_phase_telemetry = "true"
  disable_uv_wrapper = "true"
  enforce_serverless = "true"

  if enforce_serverless == "true" and os.environ.get("IS_SERVERLESS", "").strip().lower() != "true":
    raise RuntimeError("Databricks CLI runner must run on serverless compute")

  setup_runner_environment_start = time.monotonic()
  ctx = get_context()
  token = ctx.apiToken
  workspace_url = f"https://{spark.conf.get('spark.databricks.workspaceUrl')}"
  cpu_arch = "linux_arm64" if platform.machine() == "aarch64" else "linux_amd64"

  env = os.environ.copy()

  def emit_event(phase, start_time):
    if enable_phase_telemetry == "true":
      print(f"\n__RUNNER_PHASE__:{phase}:{(time.monotonic() - start_time) * 1000:.0f}", flush=True)

  def get_download_url(file_api_path):
    return _cli_runner_get_download_url(
      workspace_url, token, file_api_path, runner_download_error_marker
    )

  def download_and_extract(file_url, dest_dir):
    response = requests.get(file_url)
    if response.status_code != 200:
      raise RuntimeError(f"HTTP {response.status_code} while downloading artifact: {response.text}")
    tmp_file = f"{dest_dir}.zip"

    with open(tmp_file, "wb") as zip_file:
        zip_file.write(response.content)
    with zipfile.ZipFile(tmp_file, 'r') as zip_ref:
        zip_ref.extractall(dest_dir)

  def download_artifact_if_not_exists(input_file_name, binary_name, dest_path, tmp_dir):
    if os.path.exists(dest_path):
      return

    try:
      os.makedirs(os.path.dirname(dest_path), exist_ok=True)

      path_prefix = f"/DatabricksInternal/Jobs/ArtifactRepository/public/databricks-cli-binaries/{cli_version}"
      download_url = get_download_url(f"{path_prefix}/{input_file_name}.zip")

      tmp_archive_dir = f"{tmp_dir}/{input_file_name}"
      download_and_extract(download_url, tmp_archive_dir)

      src_path = f"{tmp_archive_dir}/{binary_name}"
      if not os.path.exists(src_path):
        available = ", ".join(os.listdir(tmp_archive_dir)) if os.path.isdir(tmp_archive_dir) else "(missing archive directory)"
        raise RuntimeError(f"Expected binary '{binary_name}' not found in archive contents: {available}")

      shutil.move(src_path, dest_path)
      os.chmod(dest_path, 0o755)
    except Exception as e:
      error_msg = f"{artifact_download_error_prefix} {input_file_name}/{binary_name} CLI v{cli_version}, TF v{terraform_version}, TF provider v{provider_version}: {str(e)}"
      raise RuntimeError(f"{error_msg}")

  tmp_base = os.path.expanduser("~/tmp")
  if not os.path.exists(tmp_base):
    os.makedirs(tmp_base)
  os.chmod(tmp_base, 0o700)

  with tempfile.TemporaryDirectory(dir=tmp_base) as tmpdirname:
      emit_event("setup_runner_environment", setup_runner_environment_start)

      download_artifacts_start = time.monotonic()
      cli_exec_path = f"{tmp_base}/databricks_{cli_version}_{cpu_arch}"
      download_artifact_if_not_exists(
        f"databricks_cli_{cpu_arch}",
        "databricks",
        cli_exec_path,
        tmpdirname
      )

      terraform_base = f"{tmp_base}/terraform"
      terraform_exec_path = f"{terraform_base}/terraform_{terraform_version}_{cpu_arch}"
      download_artifact_if_not_exists(
        f"terraform_{cpu_arch}",
        "terraform",
        terraform_exec_path,
        tmpdirname
      )

      provider_mirror_base = f"{terraform_base}/providers"
      provider_mirror_dir = f"{provider_mirror_base}/registry.terraform.io/databricks/databricks/{provider_version}/{cpu_arch}"
      os.makedirs(provider_mirror_dir, exist_ok=True)
      provider_exec_path = f"{provider_mirror_dir}/terraform-provider-databricks_v{provider_version}"
      download_artifact_if_not_exists(
        f"terraform_provider_{cpu_arch}",
        f"terraform-provider-databricks_v{provider_version}",
        provider_exec_path,
        tmpdirname
      )

      if disable_uv_wrapper != "true":
        uv_base = f"{tmp_base}/uv"
        uv_exec_path = f"{uv_base}/uv_{uv_version}_{cpu_arch}"
        try:
            download_artifact_if_not_exists(
              f"uv_{cpu_arch}",
              "uv",
              uv_exec_path,
              tmpdirname
            )
        except Exception:
            pass
      else:
        uv_exec_path = "uv"
      emit_event("download_artifacts", download_artifacts_start)

      setup_cli_environment_start = time.monotonic()
      if disable_uv_wrapper != "true":
        _cli_runner_create_uv_wrapper(uv_exec_path, uv_version, env)

      terraform_cli_config_path = f"{terraform_base}/terraform-cli-config"
      if not os.path.exists(terraform_cli_config_path):
        terraform_cli_config = f'''provider_installation {{
  filesystem_mirror {{
    path    = "{provider_mirror_base}"
    include = ["registry.terraform.io/databricks/databricks"]
  }}
  direct {{
    exclude = ["registry.terraform.io/databricks/databricks"]
  }}
}}
disable_checkpoint = true
disable_checkpoint_signature = true
'''

        with open(terraform_cli_config_path, 'w') as f:
          f.write(terraform_cli_config)

      cwd = os.path.normpath(f"/Workspace{cwd}")

      env["DATABRICKS_TF_VERSION"] = terraform_version
      env["DATABRICKS_TF_EXEC_PATH"] = terraform_exec_path
      env["DATABRICKS_TF_PROVIDER_VERSION"] = provider_version
      env["DATABRICKS_TF_CLI_CONFIG_FILE"] = terraform_cli_config_path
      env["TF_CLI_CONFIG_FILE"] = terraform_cli_config_path

      if dev_cli_snippet:
        cli_path = cli_exec_path
        exec(dev_cli_snippet, globals(), locals())

      env["DATABRICKS_BUNDLE_TMP"] = f"{tmp_base}/bundle-2-{hashlib.sha256(cwd.encode()).hexdigest()}"
      env["DATABRICKS_CLI_UPSTREAM"] = cli_upstream

      if enable_yaml_sync == "true":
        env["DATABRICKS_BUNDLE_ENABLE_EXPERIMENTAL_YAML_SYNC"] = "1"
      else:
        env.pop("DATABRICKS_BUNDLE_ENABLE_EXPERIMENTAL_YAML_SYNC", None)

      if enable_direct_engine == "true":
        env["DATABRICKS_BUNDLE_ENGINE"] = "direct"
      else:
        env.pop("DATABRICKS_BUNDLE_ENGINE", None)

      if force_allow_record_deployment_history == "true":
        env["DATABRICKS_BUNDLE_FORCE_ALLOW_RECORD_DEPLOYMENT_HISTORY"] = "true"
        env["DATABRICKS_BUNDLE_INIT_DEPLOYMENT_HISTORY"] = "true"
      else:
        env.pop("DATABRICKS_BUNDLE_FORCE_ALLOW_RECORD_DEPLOYMENT_HISTORY", None)
        env.pop("DATABRICKS_BUNDLE_INIT_DEPLOYMENT_HISTORY", None)

      emit_event("setup_cli_environment", setup_cli_environment_start)

      if allow_python_execution == "true":
        install_dependencies_start = time.monotonic()
        venv_path = _cli_runner_get_venv_path_from_databricks_yml(cwd)
        venv_folder_path = os.path.normpath(f"{cwd}/{venv_path}")
        venv_bin_path = f"{venv_folder_path}/bin"

        os.makedirs(venv_bin_path, exist_ok=True)

        python3_wrapper_path = f"{venv_bin_path}/python3"
        if not os.path.exists(python3_wrapper_path):
          with open(python3_wrapper_path, "w") as f:
            f.write('#!/usr/bin/env bash\nexec python3 "$@"\n')
          os.chmod(python3_wrapper_path, 0o755)

        pyvenv_cfg_path = f"{venv_folder_path}/pyvenv.cfg"
        if not os.path.exists(pyvenv_cfg_path):
          with open(pyvenv_cfg_path, "w") as f:
            pass

        pyproject_path = f"{cwd}/pyproject.toml"
        uv_pip_install_args = [uv_exec_path, "pip", "install", "--python", python3_wrapper_path, "--link-mode=copy"]

        override_file = None
        if os.path.exists(pyproject_path):
          uv_pip_install_args.extend(["-r", pyproject_path, "--group", "dev"])
        else:
          uv_pip_install_args.append(f"databricks-bundles=={cli_version}")

        try:
          subprocess.run(uv_pip_install_args, check=True, cwd=cwd, stdout=subprocess.DEVNULL, env=env)
        finally:
          if override_file and os.path.exists(override_file):
            os.remove(override_file)
        emit_event("install_dependencies", install_dependencies_start)

        env.pop("DATABRICKS_BUNDLE_RESTRICTED_CODE_EXECUTION", None)
      else:
        env["DATABRICKS_BUNDLE_RESTRICTED_CODE_EXECUTION"] = "1"

      env.pop("DATABRICKS_CLUSTER_ID", None)

      with tempfile.NamedTemporaryFile(mode='w+', dir=tmpdirname, delete=True) as databrickscfg_file:
          profile_name = "workspace_profile"
          databrickscfg_file.write(f"[workspace_profile]\nhost = {workspace_url}")
          databrickscfg_file.flush()
          env["DATABRICKS_CONFIG_FILE"] = databrickscfg_file.name

          cli_command = [cli_exec_path] + shlex.split(args) + ["-p", profile_name]
          tmp_file_path = tmp_file

          run_cli_start = time.monotonic()
          run_cli_phase = "run_cli_genie" if cli_upstream == "genie-code" else "run_cli"
          try:
            _cli_runner_run_cli_command_with_token(
              cli_command,
              cwd,
              tmp_file_path,
              include_stderr_in_error == "true",
              token,
              env,
              cli_upstream=cli_upstream,
            )
          finally:
            emit_event(run_cli_phase, run_cli_start)


try:
  _databricks_cli_runner()
finally:
  del (
    _databricks_cli_runner,
    _cli_runner_create_uv_wrapper,
    _cli_runner_get_venv_path_from_databricks_yml,
    _cli_runner_get_download_url,
    _cli_runner_run_cli_command,
    _cli_runner_run_cli_command_with_token,
  )
None


# COMMAND ----------

def _cli_runner_create_uv_wrapper(uv_exec_path, uv_version, env):
  import os

  uv_wrapper_code = f"""#!/bin/bash -e
if [[ ! -f "{uv_exec_path}" ]]; then
  curl -LsSf https://astral.sh/uv/{uv_version}/install.sh | sh
  exec "$HOME/.local/bin/uv" "$@"
fi
exec "{uv_exec_path}" "$@"
"""
  uv_wrapper_path = "uv"
  with open(uv_wrapper_path, "w") as file:
    file.write(uv_wrapper_code)
  os.chmod(uv_wrapper_path, 0o755)
  env["PATH"] = env.get("PATH", "") + os.pathsep + os.getcwd()


def _cli_runner_get_venv_path_from_databricks_yml(cwd):
  import yaml

  databricks_yml_path = f"{cwd}/databricks.yml"
  with open(databricks_yml_path, "r") as f:
    data = yaml.safe_load(f)
    return data.get("python", {}).get("venv_path", ".venv")


def _cli_runner_run_cli_command(cli_command, cwd, tmp_file_path, include_stderr, env, *, cli_upstream):
  import os
  import subprocess

  stderr_arg = subprocess.PIPE if include_stderr else None
  if tmp_file_path:
    os.makedirs(os.path.dirname(tmp_file_path), exist_ok=True)
    result = subprocess.run(cli_command, cwd=cwd, stdout=subprocess.PIPE, stderr=stderr_arg, text=True, env=env)
    with open(tmp_file_path, 'w') as output_file:
        output_file.write(result.stdout)
        print(result.stdout)
  else:
    result = subprocess.run(cli_command, cwd=cwd, stderr=stderr_arg, text=True, env=env)
  if result.returncode != 0:
    error_msg = f"Command {result.args[:3]} failed with exit code {result.returncode}"
    if include_stderr and result.stderr:
      error_msg += f":\n{result.stderr}"
    raise RuntimeError(error_msg)

  if cli_upstream == "genie-code":
    print(result.stderr)

    try:
      with open("/Workspace/.proc/self/status", "r") as status_file:
        status_file.read()
    except Exception as e:
      print(f"Warning: Failed to read /Workspace/.proc/self/status: {e}")


def _cli_runner_run_cli_command_with_token(cli_command, cwd, tmp_file_path, include_stderr, token, env, *, cli_upstream):
  _cli_runner_run_cli_command(
    cli_command,
    cwd,
    tmp_file_path,
    include_stderr,
    {**env, "DATABRICKS_TOKEN": token},
    cli_upstream=cli_upstream,
  )


def _cli_runner_get_download_url(workspace_url, token, file_api_path, runner_download_error_marker):
  import requests
  import time

  api_create_download_url = f"{workspace_url}/api/2.0/fs/create-download-url"
  headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
  data = {"path": file_api_path}

  max_attempts = 3
  last_error = None
  for attempt in range(max_attempts):
    try:
      response = requests.post(api_create_download_url, headers=headers, json=data)
      if response.status_code == 200:
        return response.json()["url"]
      last_error = f"HTTP {response.status_code} {response.text}"
    except requests.RequestException as e:
      last_error = str(e)
    if attempt < max_attempts - 1:
      time.sleep(2 ** attempt)

  print(f"{runner_download_error_marker}: {file_api_path} {last_error}")
  raise RuntimeError(f"Failed to download {file_api_path}: {last_error}")


def _databricks_cli_runner():
  import hashlib
  import os
  import platform
  import requests
  import shlex
  import shutil
  import subprocess
  import tempfile
  import time
  import zipfile
  from dbruntime.databricks_repl_context import get_context

  cli_version = "1.17.0"
  terraform_version = "1.5.5"
  provider_version = "1.132.0"
  uv_version = "0.6.10"
  cwd = "/Repos/jakkularajeswari266@gmail.com/adb_cicd"
  args = "bundle summary -o json --include-locations --target dev"
  tmp_file = "/Workspace/Users/jakkularajeswari266@gmail.com/.bundle/DABDemo/.output/summary-output-egq75r5gnk.json"
  dev_cli_snippet = ""
  artifact_download_error_prefix = "DABsRunnerError: Failed to download CLI dependency"
  runner_download_error_marker = "DabsRunnerErrorFallback: Downloading error, using fallback URL"
  allow_python_execution = "false"
  enable_yaml_sync = "true"
  enable_direct_engine = "true"
  force_allow_record_deployment_history = "false"
  include_stderr_in_error = "false"
  cli_upstream = "dabs-in-workspace"
  enable_phase_telemetry = "true"
  disable_uv_wrapper = "true"
  enforce_serverless = "true"

  if enforce_serverless == "true" and os.environ.get("IS_SERVERLESS", "").strip().lower() != "true":
    raise RuntimeError("Databricks CLI runner must run on serverless compute")

  setup_runner_environment_start = time.monotonic()
  ctx = get_context()
  token = ctx.apiToken
  workspace_url = f"https://{spark.conf.get('spark.databricks.workspaceUrl')}"
  cpu_arch = "linux_arm64" if platform.machine() == "aarch64" else "linux_amd64"

  env = os.environ.copy()

  def emit_event(phase, start_time):
    if enable_phase_telemetry == "true":
      print(f"\n__RUNNER_PHASE__:{phase}:{(time.monotonic() - start_time) * 1000:.0f}", flush=True)

  def get_download_url(file_api_path):
    return _cli_runner_get_download_url(
      workspace_url, token, file_api_path, runner_download_error_marker
    )

  def download_and_extract(file_url, dest_dir):
    response = requests.get(file_url)
    if response.status_code != 200:
      raise RuntimeError(f"HTTP {response.status_code} while downloading artifact: {response.text}")
    tmp_file = f"{dest_dir}.zip"

    with open(tmp_file, "wb") as zip_file:
        zip_file.write(response.content)
    with zipfile.ZipFile(tmp_file, 'r') as zip_ref:
        zip_ref.extractall(dest_dir)

  def download_artifact_if_not_exists(input_file_name, binary_name, dest_path, tmp_dir):
    if os.path.exists(dest_path):
      return

    try:
      os.makedirs(os.path.dirname(dest_path), exist_ok=True)

      path_prefix = f"/DatabricksInternal/Jobs/ArtifactRepository/public/databricks-cli-binaries/{cli_version}"
      download_url = get_download_url(f"{path_prefix}/{input_file_name}.zip")

      tmp_archive_dir = f"{tmp_dir}/{input_file_name}"
      download_and_extract(download_url, tmp_archive_dir)

      src_path = f"{tmp_archive_dir}/{binary_name}"
      if not os.path.exists(src_path):
        available = ", ".join(os.listdir(tmp_archive_dir)) if os.path.isdir(tmp_archive_dir) else "(missing archive directory)"
        raise RuntimeError(f"Expected binary '{binary_name}' not found in archive contents: {available}")

      shutil.move(src_path, dest_path)
      os.chmod(dest_path, 0o755)
    except Exception as e:
      error_msg = f"{artifact_download_error_prefix} {input_file_name}/{binary_name} CLI v{cli_version}, TF v{terraform_version}, TF provider v{provider_version}: {str(e)}"
      raise RuntimeError(f"{error_msg}")

  tmp_base = os.path.expanduser("~/tmp")
  if not os.path.exists(tmp_base):
    os.makedirs(tmp_base)
  os.chmod(tmp_base, 0o700)

  with tempfile.TemporaryDirectory(dir=tmp_base) as tmpdirname:
      emit_event("setup_runner_environment", setup_runner_environment_start)

      download_artifacts_start = time.monotonic()
      cli_exec_path = f"{tmp_base}/databricks_{cli_version}_{cpu_arch}"
      download_artifact_if_not_exists(
        f"databricks_cli_{cpu_arch}",
        "databricks",
        cli_exec_path,
        tmpdirname
      )

      terraform_base = f"{tmp_base}/terraform"
      terraform_exec_path = f"{terraform_base}/terraform_{terraform_version}_{cpu_arch}"
      download_artifact_if_not_exists(
        f"terraform_{cpu_arch}",
        "terraform",
        terraform_exec_path,
        tmpdirname
      )

      provider_mirror_base = f"{terraform_base}/providers"
      provider_mirror_dir = f"{provider_mirror_base}/registry.terraform.io/databricks/databricks/{provider_version}/{cpu_arch}"
      os.makedirs(provider_mirror_dir, exist_ok=True)
      provider_exec_path = f"{provider_mirror_dir}/terraform-provider-databricks_v{provider_version}"
      download_artifact_if_not_exists(
        f"terraform_provider_{cpu_arch}",
        f"terraform-provider-databricks_v{provider_version}",
        provider_exec_path,
        tmpdirname
      )

      if disable_uv_wrapper != "true":
        uv_base = f"{tmp_base}/uv"
        uv_exec_path = f"{uv_base}/uv_{uv_version}_{cpu_arch}"
        try:
            download_artifact_if_not_exists(
              f"uv_{cpu_arch}",
              "uv",
              uv_exec_path,
              tmpdirname
            )
        except Exception:
            pass
      else:
        uv_exec_path = "uv"
      emit_event("download_artifacts", download_artifacts_start)

      setup_cli_environment_start = time.monotonic()
      if disable_uv_wrapper != "true":
        _cli_runner_create_uv_wrapper(uv_exec_path, uv_version, env)

      terraform_cli_config_path = f"{terraform_base}/terraform-cli-config"
      if not os.path.exists(terraform_cli_config_path):
        terraform_cli_config = f'''provider_installation {{
  filesystem_mirror {{
    path    = "{provider_mirror_base}"
    include = ["registry.terraform.io/databricks/databricks"]
  }}
  direct {{
    exclude = ["registry.terraform.io/databricks/databricks"]
  }}
}}
disable_checkpoint = true
disable_checkpoint_signature = true
'''

        with open(terraform_cli_config_path, 'w') as f:
          f.write(terraform_cli_config)

      cwd = os.path.normpath(f"/Workspace{cwd}")

      env["DATABRICKS_TF_VERSION"] = terraform_version
      env["DATABRICKS_TF_EXEC_PATH"] = terraform_exec_path
      env["DATABRICKS_TF_PROVIDER_VERSION"] = provider_version
      env["DATABRICKS_TF_CLI_CONFIG_FILE"] = terraform_cli_config_path
      env["TF_CLI_CONFIG_FILE"] = terraform_cli_config_path

      if dev_cli_snippet:
        cli_path = cli_exec_path
        exec(dev_cli_snippet, globals(), locals())

      env["DATABRICKS_BUNDLE_TMP"] = f"{tmp_base}/bundle-2-{hashlib.sha256(cwd.encode()).hexdigest()}"
      env["DATABRICKS_CLI_UPSTREAM"] = cli_upstream

      if enable_yaml_sync == "true":
        env["DATABRICKS_BUNDLE_ENABLE_EXPERIMENTAL_YAML_SYNC"] = "1"
      else:
        env.pop("DATABRICKS_BUNDLE_ENABLE_EXPERIMENTAL_YAML_SYNC", None)

      if enable_direct_engine == "true":
        env["DATABRICKS_BUNDLE_ENGINE"] = "direct"
      else:
        env.pop("DATABRICKS_BUNDLE_ENGINE", None)

      if force_allow_record_deployment_history == "true":
        env["DATABRICKS_BUNDLE_FORCE_ALLOW_RECORD_DEPLOYMENT_HISTORY"] = "true"
        env["DATABRICKS_BUNDLE_INIT_DEPLOYMENT_HISTORY"] = "true"
      else:
        env.pop("DATABRICKS_BUNDLE_FORCE_ALLOW_RECORD_DEPLOYMENT_HISTORY", None)
        env.pop("DATABRICKS_BUNDLE_INIT_DEPLOYMENT_HISTORY", None)

      emit_event("setup_cli_environment", setup_cli_environment_start)

      if allow_python_execution == "true":
        install_dependencies_start = time.monotonic()
        venv_path = _cli_runner_get_venv_path_from_databricks_yml(cwd)
        venv_folder_path = os.path.normpath(f"{cwd}/{venv_path}")
        venv_bin_path = f"{venv_folder_path}/bin"

        os.makedirs(venv_bin_path, exist_ok=True)

        python3_wrapper_path = f"{venv_bin_path}/python3"
        if not os.path.exists(python3_wrapper_path):
          with open(python3_wrapper_path, "w") as f:
            f.write('#!/usr/bin/env bash\nexec python3 "$@"\n')
          os.chmod(python3_wrapper_path, 0o755)

        pyvenv_cfg_path = f"{venv_folder_path}/pyvenv.cfg"
        if not os.path.exists(pyvenv_cfg_path):
          with open(pyvenv_cfg_path, "w") as f:
            pass

        pyproject_path = f"{cwd}/pyproject.toml"
        uv_pip_install_args = [uv_exec_path, "pip", "install", "--python", python3_wrapper_path, "--link-mode=copy"]

        override_file = None
        if os.path.exists(pyproject_path):
          uv_pip_install_args.extend(["-r", pyproject_path, "--group", "dev"])
        else:
          uv_pip_install_args.append(f"databricks-bundles=={cli_version}")

        try:
          subprocess.run(uv_pip_install_args, check=True, cwd=cwd, stdout=subprocess.DEVNULL, env=env)
        finally:
          if override_file and os.path.exists(override_file):
            os.remove(override_file)
        emit_event("install_dependencies", install_dependencies_start)

        env.pop("DATABRICKS_BUNDLE_RESTRICTED_CODE_EXECUTION", None)
      else:
        env["DATABRICKS_BUNDLE_RESTRICTED_CODE_EXECUTION"] = "1"

      env.pop("DATABRICKS_CLUSTER_ID", None)

      with tempfile.NamedTemporaryFile(mode='w+', dir=tmpdirname, delete=True) as databrickscfg_file:
          profile_name = "workspace_profile"
          databrickscfg_file.write(f"[workspace_profile]\nhost = {workspace_url}")
          databrickscfg_file.flush()
          env["DATABRICKS_CONFIG_FILE"] = databrickscfg_file.name

          cli_command = [cli_exec_path] + shlex.split(args) + ["-p", profile_name]
          tmp_file_path = tmp_file

          run_cli_start = time.monotonic()
          run_cli_phase = "run_cli_genie" if cli_upstream == "genie-code" else "run_cli"
          try:
            _cli_runner_run_cli_command_with_token(
              cli_command,
              cwd,
              tmp_file_path,
              include_stderr_in_error == "true",
              token,
              env,
              cli_upstream=cli_upstream,
            )
          finally:
            emit_event(run_cli_phase, run_cli_start)


try:
  _databricks_cli_runner()
finally:
  del (
    _databricks_cli_runner,
    _cli_runner_create_uv_wrapper,
    _cli_runner_get_venv_path_from_databricks_yml,
    _cli_runner_get_download_url,
    _cli_runner_run_cli_command,
    _cli_runner_run_cli_command_with_token,
  )
None


# COMMAND ----------

def _cli_runner_create_uv_wrapper(uv_exec_path, uv_version, env):
  import os

  uv_wrapper_code = f"""#!/bin/bash -e
if [[ ! -f "{uv_exec_path}" ]]; then
  curl -LsSf https://astral.sh/uv/{uv_version}/install.sh | sh
  exec "$HOME/.local/bin/uv" "$@"
fi
exec "{uv_exec_path}" "$@"
"""
  uv_wrapper_path = "uv"
  with open(uv_wrapper_path, "w") as file:
    file.write(uv_wrapper_code)
  os.chmod(uv_wrapper_path, 0o755)
  env["PATH"] = env.get("PATH", "") + os.pathsep + os.getcwd()


def _cli_runner_get_venv_path_from_databricks_yml(cwd):
  import yaml

  databricks_yml_path = f"{cwd}/databricks.yml"
  with open(databricks_yml_path, "r") as f:
    data = yaml.safe_load(f)
    return data.get("python", {}).get("venv_path", ".venv")


def _cli_runner_run_cli_command(cli_command, cwd, tmp_file_path, include_stderr, env, *, cli_upstream):
  import os
  import subprocess

  stderr_arg = subprocess.PIPE if include_stderr else None
  if tmp_file_path:
    os.makedirs(os.path.dirname(tmp_file_path), exist_ok=True)
    result = subprocess.run(cli_command, cwd=cwd, stdout=subprocess.PIPE, stderr=stderr_arg, text=True, env=env)
    with open(tmp_file_path, 'w') as output_file:
        output_file.write(result.stdout)
        print(result.stdout)
  else:
    result = subprocess.run(cli_command, cwd=cwd, stderr=stderr_arg, text=True, env=env)
  if result.returncode != 0:
    error_msg = f"Command {result.args[:3]} failed with exit code {result.returncode}"
    if include_stderr and result.stderr:
      error_msg += f":\n{result.stderr}"
    raise RuntimeError(error_msg)

  if cli_upstream == "genie-code":
    print(result.stderr)

    try:
      with open("/Workspace/.proc/self/status", "r") as status_file:
        status_file.read()
    except Exception as e:
      print(f"Warning: Failed to read /Workspace/.proc/self/status: {e}")


def _cli_runner_run_cli_command_with_token(cli_command, cwd, tmp_file_path, include_stderr, token, env, *, cli_upstream):
  _cli_runner_run_cli_command(
    cli_command,
    cwd,
    tmp_file_path,
    include_stderr,
    {**env, "DATABRICKS_TOKEN": token},
    cli_upstream=cli_upstream,
  )


def _cli_runner_get_download_url(workspace_url, token, file_api_path, runner_download_error_marker):
  import requests
  import time

  api_create_download_url = f"{workspace_url}/api/2.0/fs/create-download-url"
  headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
  data = {"path": file_api_path}

  max_attempts = 3
  last_error = None
  for attempt in range(max_attempts):
    try:
      response = requests.post(api_create_download_url, headers=headers, json=data)
      if response.status_code == 200:
        return response.json()["url"]
      last_error = f"HTTP {response.status_code} {response.text}"
    except requests.RequestException as e:
      last_error = str(e)
    if attempt < max_attempts - 1:
      time.sleep(2 ** attempt)

  print(f"{runner_download_error_marker}: {file_api_path} {last_error}")
  raise RuntimeError(f"Failed to download {file_api_path}: {last_error}")


def _databricks_cli_runner():
  import hashlib
  import os
  import platform
  import requests
  import shlex
  import shutil
  import subprocess
  import tempfile
  import time
  import zipfile
  from dbruntime.databricks_repl_context import get_context

  cli_version = "1.17.0"
  terraform_version = "1.5.5"
  provider_version = "1.132.0"
  uv_version = "0.6.10"
  cwd = "/Repos/jakkularajeswari266@gmail.com/adb_cicd"
  args = "bundle summary -o json --include-locations --target dev"
  tmp_file = "/Workspace/Users/jakkularajeswari266@gmail.com/.bundle/DABDemo/.output/summary-output-p9x3espufxn.json"
  dev_cli_snippet = ""
  artifact_download_error_prefix = "DABsRunnerError: Failed to download CLI dependency"
  runner_download_error_marker = "DabsRunnerErrorFallback: Downloading error, using fallback URL"
  allow_python_execution = "false"
  enable_yaml_sync = "true"
  enable_direct_engine = "true"
  force_allow_record_deployment_history = "false"
  include_stderr_in_error = "false"
  cli_upstream = "dabs-in-workspace"
  enable_phase_telemetry = "true"
  disable_uv_wrapper = "true"
  enforce_serverless = "true"

  if enforce_serverless == "true" and os.environ.get("IS_SERVERLESS", "").strip().lower() != "true":
    raise RuntimeError("Databricks CLI runner must run on serverless compute")

  setup_runner_environment_start = time.monotonic()
  ctx = get_context()
  token = ctx.apiToken
  workspace_url = f"https://{spark.conf.get('spark.databricks.workspaceUrl')}"
  cpu_arch = "linux_arm64" if platform.machine() == "aarch64" else "linux_amd64"

  env = os.environ.copy()

  def emit_event(phase, start_time):
    if enable_phase_telemetry == "true":
      print(f"\n__RUNNER_PHASE__:{phase}:{(time.monotonic() - start_time) * 1000:.0f}", flush=True)

  def get_download_url(file_api_path):
    return _cli_runner_get_download_url(
      workspace_url, token, file_api_path, runner_download_error_marker
    )

  def download_and_extract(file_url, dest_dir):
    response = requests.get(file_url)
    if response.status_code != 200:
      raise RuntimeError(f"HTTP {response.status_code} while downloading artifact: {response.text}")
    tmp_file = f"{dest_dir}.zip"

    with open(tmp_file, "wb") as zip_file:
        zip_file.write(response.content)
    with zipfile.ZipFile(tmp_file, 'r') as zip_ref:
        zip_ref.extractall(dest_dir)

  def download_artifact_if_not_exists(input_file_name, binary_name, dest_path, tmp_dir):
    if os.path.exists(dest_path):
      return

    try:
      os.makedirs(os.path.dirname(dest_path), exist_ok=True)

      path_prefix = f"/DatabricksInternal/Jobs/ArtifactRepository/public/databricks-cli-binaries/{cli_version}"
      download_url = get_download_url(f"{path_prefix}/{input_file_name}.zip")

      tmp_archive_dir = f"{tmp_dir}/{input_file_name}"
      download_and_extract(download_url, tmp_archive_dir)

      src_path = f"{tmp_archive_dir}/{binary_name}"
      if not os.path.exists(src_path):
        available = ", ".join(os.listdir(tmp_archive_dir)) if os.path.isdir(tmp_archive_dir) else "(missing archive directory)"
        raise RuntimeError(f"Expected binary '{binary_name}' not found in archive contents: {available}")

      shutil.move(src_path, dest_path)
      os.chmod(dest_path, 0o755)
    except Exception as e:
      error_msg = f"{artifact_download_error_prefix} {input_file_name}/{binary_name} CLI v{cli_version}, TF v{terraform_version}, TF provider v{provider_version}: {str(e)}"
      raise RuntimeError(f"{error_msg}")

  tmp_base = os.path.expanduser("~/tmp")
  if not os.path.exists(tmp_base):
    os.makedirs(tmp_base)
  os.chmod(tmp_base, 0o700)

  with tempfile.TemporaryDirectory(dir=tmp_base) as tmpdirname:
      emit_event("setup_runner_environment", setup_runner_environment_start)

      download_artifacts_start = time.monotonic()
      cli_exec_path = f"{tmp_base}/databricks_{cli_version}_{cpu_arch}"
      download_artifact_if_not_exists(
        f"databricks_cli_{cpu_arch}",
        "databricks",
        cli_exec_path,
        tmpdirname
      )

      terraform_base = f"{tmp_base}/terraform"
      terraform_exec_path = f"{terraform_base}/terraform_{terraform_version}_{cpu_arch}"
      download_artifact_if_not_exists(
        f"terraform_{cpu_arch}",
        "terraform",
        terraform_exec_path,
        tmpdirname
      )

      provider_mirror_base = f"{terraform_base}/providers"
      provider_mirror_dir = f"{provider_mirror_base}/registry.terraform.io/databricks/databricks/{provider_version}/{cpu_arch}"
      os.makedirs(provider_mirror_dir, exist_ok=True)
      provider_exec_path = f"{provider_mirror_dir}/terraform-provider-databricks_v{provider_version}"
      download_artifact_if_not_exists(
        f"terraform_provider_{cpu_arch}",
        f"terraform-provider-databricks_v{provider_version}",
        provider_exec_path,
        tmpdirname
      )

      if disable_uv_wrapper != "true":
        uv_base = f"{tmp_base}/uv"
        uv_exec_path = f"{uv_base}/uv_{uv_version}_{cpu_arch}"
        try:
            download_artifact_if_not_exists(
              f"uv_{cpu_arch}",
              "uv",
              uv_exec_path,
              tmpdirname
            )
        except Exception:
            pass
      else:
        uv_exec_path = "uv"
      emit_event("download_artifacts", download_artifacts_start)

      setup_cli_environment_start = time.monotonic()
      if disable_uv_wrapper != "true":
        _cli_runner_create_uv_wrapper(uv_exec_path, uv_version, env)

      terraform_cli_config_path = f"{terraform_base}/terraform-cli-config"
      if not os.path.exists(terraform_cli_config_path):
        terraform_cli_config = f'''provider_installation {{
  filesystem_mirror {{
    path    = "{provider_mirror_base}"
    include = ["registry.terraform.io/databricks/databricks"]
  }}
  direct {{
    exclude = ["registry.terraform.io/databricks/databricks"]
  }}
}}
disable_checkpoint = true
disable_checkpoint_signature = true
'''

        with open(terraform_cli_config_path, 'w') as f:
          f.write(terraform_cli_config)

      cwd = os.path.normpath(f"/Workspace{cwd}")

      env["DATABRICKS_TF_VERSION"] = terraform_version
      env["DATABRICKS_TF_EXEC_PATH"] = terraform_exec_path
      env["DATABRICKS_TF_PROVIDER_VERSION"] = provider_version
      env["DATABRICKS_TF_CLI_CONFIG_FILE"] = terraform_cli_config_path
      env["TF_CLI_CONFIG_FILE"] = terraform_cli_config_path

      if dev_cli_snippet:
        cli_path = cli_exec_path
        exec(dev_cli_snippet, globals(), locals())

      env["DATABRICKS_BUNDLE_TMP"] = f"{tmp_base}/bundle-2-{hashlib.sha256(cwd.encode()).hexdigest()}"
      env["DATABRICKS_CLI_UPSTREAM"] = cli_upstream

      if enable_yaml_sync == "true":
        env["DATABRICKS_BUNDLE_ENABLE_EXPERIMENTAL_YAML_SYNC"] = "1"
      else:
        env.pop("DATABRICKS_BUNDLE_ENABLE_EXPERIMENTAL_YAML_SYNC", None)

      if enable_direct_engine == "true":
        env["DATABRICKS_BUNDLE_ENGINE"] = "direct"
      else:
        env.pop("DATABRICKS_BUNDLE_ENGINE", None)

      if force_allow_record_deployment_history == "true":
        env["DATABRICKS_BUNDLE_FORCE_ALLOW_RECORD_DEPLOYMENT_HISTORY"] = "true"
        env["DATABRICKS_BUNDLE_INIT_DEPLOYMENT_HISTORY"] = "true"
      else:
        env.pop("DATABRICKS_BUNDLE_FORCE_ALLOW_RECORD_DEPLOYMENT_HISTORY", None)
        env.pop("DATABRICKS_BUNDLE_INIT_DEPLOYMENT_HISTORY", None)

      emit_event("setup_cli_environment", setup_cli_environment_start)

      if allow_python_execution == "true":
        install_dependencies_start = time.monotonic()
        venv_path = _cli_runner_get_venv_path_from_databricks_yml(cwd)
        venv_folder_path = os.path.normpath(f"{cwd}/{venv_path}")
        venv_bin_path = f"{venv_folder_path}/bin"

        os.makedirs(venv_bin_path, exist_ok=True)

        python3_wrapper_path = f"{venv_bin_path}/python3"
        if not os.path.exists(python3_wrapper_path):
          with open(python3_wrapper_path, "w") as f:
            f.write('#!/usr/bin/env bash\nexec python3 "$@"\n')
          os.chmod(python3_wrapper_path, 0o755)

        pyvenv_cfg_path = f"{venv_folder_path}/pyvenv.cfg"
        if not os.path.exists(pyvenv_cfg_path):
          with open(pyvenv_cfg_path, "w") as f:
            pass

        pyproject_path = f"{cwd}/pyproject.toml"
        uv_pip_install_args = [uv_exec_path, "pip", "install", "--python", python3_wrapper_path, "--link-mode=copy"]

        override_file = None
        if os.path.exists(pyproject_path):
          uv_pip_install_args.extend(["-r", pyproject_path, "--group", "dev"])
        else:
          uv_pip_install_args.append(f"databricks-bundles=={cli_version}")

        try:
          subprocess.run(uv_pip_install_args, check=True, cwd=cwd, stdout=subprocess.DEVNULL, env=env)
        finally:
          if override_file and os.path.exists(override_file):
            os.remove(override_file)
        emit_event("install_dependencies", install_dependencies_start)

        env.pop("DATABRICKS_BUNDLE_RESTRICTED_CODE_EXECUTION", None)
      else:
        env["DATABRICKS_BUNDLE_RESTRICTED_CODE_EXECUTION"] = "1"

      env.pop("DATABRICKS_CLUSTER_ID", None)

      with tempfile.NamedTemporaryFile(mode='w+', dir=tmpdirname, delete=True) as databrickscfg_file:
          profile_name = "workspace_profile"
          databrickscfg_file.write(f"[workspace_profile]\nhost = {workspace_url}")
          databrickscfg_file.flush()
          env["DATABRICKS_CONFIG_FILE"] = databrickscfg_file.name

          cli_command = [cli_exec_path] + shlex.split(args) + ["-p", profile_name]
          tmp_file_path = tmp_file

          run_cli_start = time.monotonic()
          run_cli_phase = "run_cli_genie" if cli_upstream == "genie-code" else "run_cli"
          try:
            _cli_runner_run_cli_command_with_token(
              cli_command,
              cwd,
              tmp_file_path,
              include_stderr_in_error == "true",
              token,
              env,
              cli_upstream=cli_upstream,
            )
          finally:
            emit_event(run_cli_phase, run_cli_start)


try:
  _databricks_cli_runner()
finally:
  del (
    _databricks_cli_runner,
    _cli_runner_create_uv_wrapper,
    _cli_runner_get_venv_path_from_databricks_yml,
    _cli_runner_get_download_url,
    _cli_runner_run_cli_command,
    _cli_runner_run_cli_command_with_token,
  )
None


# COMMAND ----------

def _cli_runner_create_uv_wrapper(uv_exec_path, uv_version, env):
  import os

  uv_wrapper_code = f"""#!/bin/bash -e
if [[ ! -f "{uv_exec_path}" ]]; then
  curl -LsSf https://astral.sh/uv/{uv_version}/install.sh | sh
  exec "$HOME/.local/bin/uv" "$@"
fi
exec "{uv_exec_path}" "$@"
"""
  uv_wrapper_path = "uv"
  with open(uv_wrapper_path, "w") as file:
    file.write(uv_wrapper_code)
  os.chmod(uv_wrapper_path, 0o755)
  env["PATH"] = env.get("PATH", "") + os.pathsep + os.getcwd()


def _cli_runner_get_venv_path_from_databricks_yml(cwd):
  import yaml

  databricks_yml_path = f"{cwd}/databricks.yml"
  with open(databricks_yml_path, "r") as f:
    data = yaml.safe_load(f)
    return data.get("python", {}).get("venv_path", ".venv")


def _cli_runner_run_cli_command(cli_command, cwd, tmp_file_path, include_stderr, env, *, cli_upstream):
  import os
  import subprocess

  stderr_arg = subprocess.PIPE if include_stderr else None
  if tmp_file_path:
    os.makedirs(os.path.dirname(tmp_file_path), exist_ok=True)
    result = subprocess.run(cli_command, cwd=cwd, stdout=subprocess.PIPE, stderr=stderr_arg, text=True, env=env)
    with open(tmp_file_path, 'w') as output_file:
        output_file.write(result.stdout)
        print(result.stdout)
  else:
    result = subprocess.run(cli_command, cwd=cwd, stderr=stderr_arg, text=True, env=env)
  if result.returncode != 0:
    error_msg = f"Command {result.args[:3]} failed with exit code {result.returncode}"
    if include_stderr and result.stderr:
      error_msg += f":\n{result.stderr}"
    raise RuntimeError(error_msg)

  if cli_upstream == "genie-code":
    print(result.stderr)

    try:
      with open("/Workspace/.proc/self/status", "r") as status_file:
        status_file.read()
    except Exception as e:
      print(f"Warning: Failed to read /Workspace/.proc/self/status: {e}")


def _cli_runner_run_cli_command_with_token(cli_command, cwd, tmp_file_path, include_stderr, token, env, *, cli_upstream):
  _cli_runner_run_cli_command(
    cli_command,
    cwd,
    tmp_file_path,
    include_stderr,
    {**env, "DATABRICKS_TOKEN": token},
    cli_upstream=cli_upstream,
  )


def _cli_runner_get_download_url(workspace_url, token, file_api_path, runner_download_error_marker):
  import requests
  import time

  api_create_download_url = f"{workspace_url}/api/2.0/fs/create-download-url"
  headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
  data = {"path": file_api_path}

  max_attempts = 3
  last_error = None
  for attempt in range(max_attempts):
    try:
      response = requests.post(api_create_download_url, headers=headers, json=data)
      if response.status_code == 200:
        return response.json()["url"]
      last_error = f"HTTP {response.status_code} {response.text}"
    except requests.RequestException as e:
      last_error = str(e)
    if attempt < max_attempts - 1:
      time.sleep(2 ** attempt)

  print(f"{runner_download_error_marker}: {file_api_path} {last_error}")
  raise RuntimeError(f"Failed to download {file_api_path}: {last_error}")


def _databricks_cli_runner():
  import hashlib
  import os
  import platform
  import requests
  import shlex
  import shutil
  import subprocess
  import tempfile
  import time
  import zipfile
  from dbruntime.databricks_repl_context import get_context

  cli_version = "1.17.0"
  terraform_version = "1.5.5"
  provider_version = "1.132.0"
  uv_version = "0.6.10"
  cwd = "/Repos/jakkularajeswari266@gmail.com/adb_cicd"
  args = "bundle summary -o json --include-locations --target dev"
  tmp_file = "/Workspace/Users/jakkularajeswari266@gmail.com/.bundle/DABDemo/.output/summary-output-1wwusqhs0cs.json"
  dev_cli_snippet = ""
  artifact_download_error_prefix = "DABsRunnerError: Failed to download CLI dependency"
  runner_download_error_marker = "DabsRunnerErrorFallback: Downloading error, using fallback URL"
  allow_python_execution = "false"
  enable_yaml_sync = "true"
  enable_direct_engine = "true"
  force_allow_record_deployment_history = "false"
  include_stderr_in_error = "false"
  cli_upstream = "dabs-in-workspace"
  enable_phase_telemetry = "true"
  disable_uv_wrapper = "true"
  enforce_serverless = "true"

  if enforce_serverless == "true" and os.environ.get("IS_SERVERLESS", "").strip().lower() != "true":
    raise RuntimeError("Databricks CLI runner must run on serverless compute")

  setup_runner_environment_start = time.monotonic()
  ctx = get_context()
  token = ctx.apiToken
  workspace_url = f"https://{spark.conf.get('spark.databricks.workspaceUrl')}"
  cpu_arch = "linux_arm64" if platform.machine() == "aarch64" else "linux_amd64"

  env = os.environ.copy()

  def emit_event(phase, start_time):
    if enable_phase_telemetry == "true":
      print(f"\n__RUNNER_PHASE__:{phase}:{(time.monotonic() - start_time) * 1000:.0f}", flush=True)

  def get_download_url(file_api_path):
    return _cli_runner_get_download_url(
      workspace_url, token, file_api_path, runner_download_error_marker
    )

  def download_and_extract(file_url, dest_dir):
    response = requests.get(file_url)
    if response.status_code != 200:
      raise RuntimeError(f"HTTP {response.status_code} while downloading artifact: {response.text}")
    tmp_file = f"{dest_dir}.zip"

    with open(tmp_file, "wb") as zip_file:
        zip_file.write(response.content)
    with zipfile.ZipFile(tmp_file, 'r') as zip_ref:
        zip_ref.extractall(dest_dir)

  def download_artifact_if_not_exists(input_file_name, binary_name, dest_path, tmp_dir):
    if os.path.exists(dest_path):
      return

    try:
      os.makedirs(os.path.dirname(dest_path), exist_ok=True)

      path_prefix = f"/DatabricksInternal/Jobs/ArtifactRepository/public/databricks-cli-binaries/{cli_version}"
      download_url = get_download_url(f"{path_prefix}/{input_file_name}.zip")

      tmp_archive_dir = f"{tmp_dir}/{input_file_name}"
      download_and_extract(download_url, tmp_archive_dir)

      src_path = f"{tmp_archive_dir}/{binary_name}"
      if not os.path.exists(src_path):
        available = ", ".join(os.listdir(tmp_archive_dir)) if os.path.isdir(tmp_archive_dir) else "(missing archive directory)"
        raise RuntimeError(f"Expected binary '{binary_name}' not found in archive contents: {available}")

      shutil.move(src_path, dest_path)
      os.chmod(dest_path, 0o755)
    except Exception as e:
      error_msg = f"{artifact_download_error_prefix} {input_file_name}/{binary_name} CLI v{cli_version}, TF v{terraform_version}, TF provider v{provider_version}: {str(e)}"
      raise RuntimeError(f"{error_msg}")

  tmp_base = os.path.expanduser("~/tmp")
  if not os.path.exists(tmp_base):
    os.makedirs(tmp_base)
  os.chmod(tmp_base, 0o700)

  with tempfile.TemporaryDirectory(dir=tmp_base) as tmpdirname:
      emit_event("setup_runner_environment", setup_runner_environment_start)

      download_artifacts_start = time.monotonic()
      cli_exec_path = f"{tmp_base}/databricks_{cli_version}_{cpu_arch}"
      download_artifact_if_not_exists(
        f"databricks_cli_{cpu_arch}",
        "databricks",
        cli_exec_path,
        tmpdirname
      )

      terraform_base = f"{tmp_base}/terraform"
      terraform_exec_path = f"{terraform_base}/terraform_{terraform_version}_{cpu_arch}"
      download_artifact_if_not_exists(
        f"terraform_{cpu_arch}",
        "terraform",
        terraform_exec_path,
        tmpdirname
      )

      provider_mirror_base = f"{terraform_base}/providers"
      provider_mirror_dir = f"{provider_mirror_base}/registry.terraform.io/databricks/databricks/{provider_version}/{cpu_arch}"
      os.makedirs(provider_mirror_dir, exist_ok=True)
      provider_exec_path = f"{provider_mirror_dir}/terraform-provider-databricks_v{provider_version}"
      download_artifact_if_not_exists(
        f"terraform_provider_{cpu_arch}",
        f"terraform-provider-databricks_v{provider_version}",
        provider_exec_path,
        tmpdirname
      )

      if disable_uv_wrapper != "true":
        uv_base = f"{tmp_base}/uv"
        uv_exec_path = f"{uv_base}/uv_{uv_version}_{cpu_arch}"
        try:
            download_artifact_if_not_exists(
              f"uv_{cpu_arch}",
              "uv",
              uv_exec_path,
              tmpdirname
            )
        except Exception:
            pass
      else:
        uv_exec_path = "uv"
      emit_event("download_artifacts", download_artifacts_start)

      setup_cli_environment_start = time.monotonic()
      if disable_uv_wrapper != "true":
        _cli_runner_create_uv_wrapper(uv_exec_path, uv_version, env)

      terraform_cli_config_path = f"{terraform_base}/terraform-cli-config"
      if not os.path.exists(terraform_cli_config_path):
        terraform_cli_config = f'''provider_installation {{
  filesystem_mirror {{
    path    = "{provider_mirror_base}"
    include = ["registry.terraform.io/databricks/databricks"]
  }}
  direct {{
    exclude = ["registry.terraform.io/databricks/databricks"]
  }}
}}
disable_checkpoint = true
disable_checkpoint_signature = true
'''

        with open(terraform_cli_config_path, 'w') as f:
          f.write(terraform_cli_config)

      cwd = os.path.normpath(f"/Workspace{cwd}")

      env["DATABRICKS_TF_VERSION"] = terraform_version
      env["DATABRICKS_TF_EXEC_PATH"] = terraform_exec_path
      env["DATABRICKS_TF_PROVIDER_VERSION"] = provider_version
      env["DATABRICKS_TF_CLI_CONFIG_FILE"] = terraform_cli_config_path
      env["TF_CLI_CONFIG_FILE"] = terraform_cli_config_path

      if dev_cli_snippet:
        cli_path = cli_exec_path
        exec(dev_cli_snippet, globals(), locals())

      env["DATABRICKS_BUNDLE_TMP"] = f"{tmp_base}/bundle-2-{hashlib.sha256(cwd.encode()).hexdigest()}"
      env["DATABRICKS_CLI_UPSTREAM"] = cli_upstream

      if enable_yaml_sync == "true":
        env["DATABRICKS_BUNDLE_ENABLE_EXPERIMENTAL_YAML_SYNC"] = "1"
      else:
        env.pop("DATABRICKS_BUNDLE_ENABLE_EXPERIMENTAL_YAML_SYNC", None)

      if enable_direct_engine == "true":
        env["DATABRICKS_BUNDLE_ENGINE"] = "direct"
      else:
        env.pop("DATABRICKS_BUNDLE_ENGINE", None)

      if force_allow_record_deployment_history == "true":
        env["DATABRICKS_BUNDLE_FORCE_ALLOW_RECORD_DEPLOYMENT_HISTORY"] = "true"
        env["DATABRICKS_BUNDLE_INIT_DEPLOYMENT_HISTORY"] = "true"
      else:
        env.pop("DATABRICKS_BUNDLE_FORCE_ALLOW_RECORD_DEPLOYMENT_HISTORY", None)
        env.pop("DATABRICKS_BUNDLE_INIT_DEPLOYMENT_HISTORY", None)

      emit_event("setup_cli_environment", setup_cli_environment_start)

      if allow_python_execution == "true":
        install_dependencies_start = time.monotonic()
        venv_path = _cli_runner_get_venv_path_from_databricks_yml(cwd)
        venv_folder_path = os.path.normpath(f"{cwd}/{venv_path}")
        venv_bin_path = f"{venv_folder_path}/bin"

        os.makedirs(venv_bin_path, exist_ok=True)

        python3_wrapper_path = f"{venv_bin_path}/python3"
        if not os.path.exists(python3_wrapper_path):
          with open(python3_wrapper_path, "w") as f:
            f.write('#!/usr/bin/env bash\nexec python3 "$@"\n')
          os.chmod(python3_wrapper_path, 0o755)

        pyvenv_cfg_path = f"{venv_folder_path}/pyvenv.cfg"
        if not os.path.exists(pyvenv_cfg_path):
          with open(pyvenv_cfg_path, "w") as f:
            pass

        pyproject_path = f"{cwd}/pyproject.toml"
        uv_pip_install_args = [uv_exec_path, "pip", "install", "--python", python3_wrapper_path, "--link-mode=copy"]

        override_file = None
        if os.path.exists(pyproject_path):
          uv_pip_install_args.extend(["-r", pyproject_path, "--group", "dev"])
        else:
          uv_pip_install_args.append(f"databricks-bundles=={cli_version}")

        try:
          subprocess.run(uv_pip_install_args, check=True, cwd=cwd, stdout=subprocess.DEVNULL, env=env)
        finally:
          if override_file and os.path.exists(override_file):
            os.remove(override_file)
        emit_event("install_dependencies", install_dependencies_start)

        env.pop("DATABRICKS_BUNDLE_RESTRICTED_CODE_EXECUTION", None)
      else:
        env["DATABRICKS_BUNDLE_RESTRICTED_CODE_EXECUTION"] = "1"

      env.pop("DATABRICKS_CLUSTER_ID", None)

      with tempfile.NamedTemporaryFile(mode='w+', dir=tmpdirname, delete=True) as databrickscfg_file:
          profile_name = "workspace_profile"
          databrickscfg_file.write(f"[workspace_profile]\nhost = {workspace_url}")
          databrickscfg_file.flush()
          env["DATABRICKS_CONFIG_FILE"] = databrickscfg_file.name

          cli_command = [cli_exec_path] + shlex.split(args) + ["-p", profile_name]
          tmp_file_path = tmp_file

          run_cli_start = time.monotonic()
          run_cli_phase = "run_cli_genie" if cli_upstream == "genie-code" else "run_cli"
          try:
            _cli_runner_run_cli_command_with_token(
              cli_command,
              cwd,
              tmp_file_path,
              include_stderr_in_error == "true",
              token,
              env,
              cli_upstream=cli_upstream,
            )
          finally:
            emit_event(run_cli_phase, run_cli_start)


try:
  _databricks_cli_runner()
finally:
  del (
    _databricks_cli_runner,
    _cli_runner_create_uv_wrapper,
    _cli_runner_get_venv_path_from_databricks_yml,
    _cli_runner_get_download_url,
    _cli_runner_run_cli_command,
    _cli_runner_run_cli_command_with_token,
  )
None


# COMMAND ----------

def _cli_runner_create_uv_wrapper(uv_exec_path, uv_version, env):
  import os

  uv_wrapper_code = f"""#!/bin/bash -e
if [[ ! -f "{uv_exec_path}" ]]; then
  curl -LsSf https://astral.sh/uv/{uv_version}/install.sh | sh
  exec "$HOME/.local/bin/uv" "$@"
fi
exec "{uv_exec_path}" "$@"
"""
  uv_wrapper_path = "uv"
  with open(uv_wrapper_path, "w") as file:
    file.write(uv_wrapper_code)
  os.chmod(uv_wrapper_path, 0o755)
  env["PATH"] = env.get("PATH", "") + os.pathsep + os.getcwd()


def _cli_runner_get_venv_path_from_databricks_yml(cwd):
  import yaml

  databricks_yml_path = f"{cwd}/databricks.yml"
  with open(databricks_yml_path, "r") as f:
    data = yaml.safe_load(f)
    return data.get("python", {}).get("venv_path", ".venv")


def _cli_runner_run_cli_command(cli_command, cwd, tmp_file_path, include_stderr, env, *, cli_upstream):
  import os
  import subprocess

  stderr_arg = subprocess.PIPE if include_stderr else None
  if tmp_file_path:
    os.makedirs(os.path.dirname(tmp_file_path), exist_ok=True)
    result = subprocess.run(cli_command, cwd=cwd, stdout=subprocess.PIPE, stderr=stderr_arg, text=True, env=env)
    with open(tmp_file_path, 'w') as output_file:
        output_file.write(result.stdout)
        print(result.stdout)
  else:
    result = subprocess.run(cli_command, cwd=cwd, stderr=stderr_arg, text=True, env=env)
  if result.returncode != 0:
    error_msg = f"Command {result.args[:3]} failed with exit code {result.returncode}"
    if include_stderr and result.stderr:
      error_msg += f":\n{result.stderr}"
    raise RuntimeError(error_msg)

  if cli_upstream == "genie-code":
    print(result.stderr)

    try:
      with open("/Workspace/.proc/self/status", "r") as status_file:
        status_file.read()
    except Exception as e:
      print(f"Warning: Failed to read /Workspace/.proc/self/status: {e}")


def _cli_runner_run_cli_command_with_token(cli_command, cwd, tmp_file_path, include_stderr, token, env, *, cli_upstream):
  _cli_runner_run_cli_command(
    cli_command,
    cwd,
    tmp_file_path,
    include_stderr,
    {**env, "DATABRICKS_TOKEN": token},
    cli_upstream=cli_upstream,
  )


def _cli_runner_get_download_url(workspace_url, token, file_api_path, runner_download_error_marker):
  import requests
  import time

  api_create_download_url = f"{workspace_url}/api/2.0/fs/create-download-url"
  headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
  data = {"path": file_api_path}

  max_attempts = 3
  last_error = None
  for attempt in range(max_attempts):
    try:
      response = requests.post(api_create_download_url, headers=headers, json=data)
      if response.status_code == 200:
        return response.json()["url"]
      last_error = f"HTTP {response.status_code} {response.text}"
    except requests.RequestException as e:
      last_error = str(e)
    if attempt < max_attempts - 1:
      time.sleep(2 ** attempt)

  print(f"{runner_download_error_marker}: {file_api_path} {last_error}")
  raise RuntimeError(f"Failed to download {file_api_path}: {last_error}")


def _databricks_cli_runner():
  import hashlib
  import os
  import platform
  import requests
  import shlex
  import shutil
  import subprocess
  import tempfile
  import time
  import zipfile
  from dbruntime.databricks_repl_context import get_context

  cli_version = "1.17.0"
  terraform_version = "1.5.5"
  provider_version = "1.132.0"
  uv_version = "0.6.10"
  cwd = "/Repos/jakkularajeswari266@gmail.com/adb_cicd"
  args = "bundle summary -o json --include-locations --target dev"
  tmp_file = "/Workspace/Users/jakkularajeswari266@gmail.com/.bundle/DABDemo/.output/summary-output-63zuu89mzq9.json"
  dev_cli_snippet = ""
  artifact_download_error_prefix = "DABsRunnerError: Failed to download CLI dependency"
  runner_download_error_marker = "DabsRunnerErrorFallback: Downloading error, using fallback URL"
  allow_python_execution = "false"
  enable_yaml_sync = "true"
  enable_direct_engine = "true"
  force_allow_record_deployment_history = "false"
  include_stderr_in_error = "false"
  cli_upstream = "dabs-in-workspace"
  enable_phase_telemetry = "true"
  disable_uv_wrapper = "true"
  enforce_serverless = "true"

  if enforce_serverless == "true" and os.environ.get("IS_SERVERLESS", "").strip().lower() != "true":
    raise RuntimeError("Databricks CLI runner must run on serverless compute")

  setup_runner_environment_start = time.monotonic()
  ctx = get_context()
  token = ctx.apiToken
  workspace_url = f"https://{spark.conf.get('spark.databricks.workspaceUrl')}"
  cpu_arch = "linux_arm64" if platform.machine() == "aarch64" else "linux_amd64"

  env = os.environ.copy()

  def emit_event(phase, start_time):
    if enable_phase_telemetry == "true":
      print(f"\n__RUNNER_PHASE__:{phase}:{(time.monotonic() - start_time) * 1000:.0f}", flush=True)

  def get_download_url(file_api_path):
    return _cli_runner_get_download_url(
      workspace_url, token, file_api_path, runner_download_error_marker
    )

  def download_and_extract(file_url, dest_dir):
    response = requests.get(file_url)
    if response.status_code != 200:
      raise RuntimeError(f"HTTP {response.status_code} while downloading artifact: {response.text}")
    tmp_file = f"{dest_dir}.zip"

    with open(tmp_file, "wb") as zip_file:
        zip_file.write(response.content)
    with zipfile.ZipFile(tmp_file, 'r') as zip_ref:
        zip_ref.extractall(dest_dir)

  def download_artifact_if_not_exists(input_file_name, binary_name, dest_path, tmp_dir):
    if os.path.exists(dest_path):
      return

    try:
      os.makedirs(os.path.dirname(dest_path), exist_ok=True)

      path_prefix = f"/DatabricksInternal/Jobs/ArtifactRepository/public/databricks-cli-binaries/{cli_version}"
      download_url = get_download_url(f"{path_prefix}/{input_file_name}.zip")

      tmp_archive_dir = f"{tmp_dir}/{input_file_name}"
      download_and_extract(download_url, tmp_archive_dir)

      src_path = f"{tmp_archive_dir}/{binary_name}"
      if not os.path.exists(src_path):
        available = ", ".join(os.listdir(tmp_archive_dir)) if os.path.isdir(tmp_archive_dir) else "(missing archive directory)"
        raise RuntimeError(f"Expected binary '{binary_name}' not found in archive contents: {available}")

      shutil.move(src_path, dest_path)
      os.chmod(dest_path, 0o755)
    except Exception as e:
      error_msg = f"{artifact_download_error_prefix} {input_file_name}/{binary_name} CLI v{cli_version}, TF v{terraform_version}, TF provider v{provider_version}: {str(e)}"
      raise RuntimeError(f"{error_msg}")

  tmp_base = os.path.expanduser("~/tmp")
  if not os.path.exists(tmp_base):
    os.makedirs(tmp_base)
  os.chmod(tmp_base, 0o700)

  with tempfile.TemporaryDirectory(dir=tmp_base) as tmpdirname:
      emit_event("setup_runner_environment", setup_runner_environment_start)

      download_artifacts_start = time.monotonic()
      cli_exec_path = f"{tmp_base}/databricks_{cli_version}_{cpu_arch}"
      download_artifact_if_not_exists(
        f"databricks_cli_{cpu_arch}",
        "databricks",
        cli_exec_path,
        tmpdirname
      )

      terraform_base = f"{tmp_base}/terraform"
      terraform_exec_path = f"{terraform_base}/terraform_{terraform_version}_{cpu_arch}"
      download_artifact_if_not_exists(
        f"terraform_{cpu_arch}",
        "terraform",
        terraform_exec_path,
        tmpdirname
      )

      provider_mirror_base = f"{terraform_base}/providers"
      provider_mirror_dir = f"{provider_mirror_base}/registry.terraform.io/databricks/databricks/{provider_version}/{cpu_arch}"
      os.makedirs(provider_mirror_dir, exist_ok=True)
      provider_exec_path = f"{provider_mirror_dir}/terraform-provider-databricks_v{provider_version}"
      download_artifact_if_not_exists(
        f"terraform_provider_{cpu_arch}",
        f"terraform-provider-databricks_v{provider_version}",
        provider_exec_path,
        tmpdirname
      )

      if disable_uv_wrapper != "true":
        uv_base = f"{tmp_base}/uv"
        uv_exec_path = f"{uv_base}/uv_{uv_version}_{cpu_arch}"
        try:
            download_artifact_if_not_exists(
              f"uv_{cpu_arch}",
              "uv",
              uv_exec_path,
              tmpdirname
            )
        except Exception:
            pass
      else:
        uv_exec_path = "uv"
      emit_event("download_artifacts", download_artifacts_start)

      setup_cli_environment_start = time.monotonic()
      if disable_uv_wrapper != "true":
        _cli_runner_create_uv_wrapper(uv_exec_path, uv_version, env)

      terraform_cli_config_path = f"{terraform_base}/terraform-cli-config"
      if not os.path.exists(terraform_cli_config_path):
        terraform_cli_config = f'''provider_installation {{
  filesystem_mirror {{
    path    = "{provider_mirror_base}"
    include = ["registry.terraform.io/databricks/databricks"]
  }}
  direct {{
    exclude = ["registry.terraform.io/databricks/databricks"]
  }}
}}
disable_checkpoint = true
disable_checkpoint_signature = true
'''

        with open(terraform_cli_config_path, 'w') as f:
          f.write(terraform_cli_config)

      cwd = os.path.normpath(f"/Workspace{cwd}")

      env["DATABRICKS_TF_VERSION"] = terraform_version
      env["DATABRICKS_TF_EXEC_PATH"] = terraform_exec_path
      env["DATABRICKS_TF_PROVIDER_VERSION"] = provider_version
      env["DATABRICKS_TF_CLI_CONFIG_FILE"] = terraform_cli_config_path
      env["TF_CLI_CONFIG_FILE"] = terraform_cli_config_path

      if dev_cli_snippet:
        cli_path = cli_exec_path
        exec(dev_cli_snippet, globals(), locals())

      env["DATABRICKS_BUNDLE_TMP"] = f"{tmp_base}/bundle-2-{hashlib.sha256(cwd.encode()).hexdigest()}"
      env["DATABRICKS_CLI_UPSTREAM"] = cli_upstream

      if enable_yaml_sync == "true":
        env["DATABRICKS_BUNDLE_ENABLE_EXPERIMENTAL_YAML_SYNC"] = "1"
      else:
        env.pop("DATABRICKS_BUNDLE_ENABLE_EXPERIMENTAL_YAML_SYNC", None)

      if enable_direct_engine == "true":
        env["DATABRICKS_BUNDLE_ENGINE"] = "direct"
      else:
        env.pop("DATABRICKS_BUNDLE_ENGINE", None)

      if force_allow_record_deployment_history == "true":
        env["DATABRICKS_BUNDLE_FORCE_ALLOW_RECORD_DEPLOYMENT_HISTORY"] = "true"
        env["DATABRICKS_BUNDLE_INIT_DEPLOYMENT_HISTORY"] = "true"
      else:
        env.pop("DATABRICKS_BUNDLE_FORCE_ALLOW_RECORD_DEPLOYMENT_HISTORY", None)
        env.pop("DATABRICKS_BUNDLE_INIT_DEPLOYMENT_HISTORY", None)

      emit_event("setup_cli_environment", setup_cli_environment_start)

      if allow_python_execution == "true":
        install_dependencies_start = time.monotonic()
        venv_path = _cli_runner_get_venv_path_from_databricks_yml(cwd)
        venv_folder_path = os.path.normpath(f"{cwd}/{venv_path}")
        venv_bin_path = f"{venv_folder_path}/bin"

        os.makedirs(venv_bin_path, exist_ok=True)

        python3_wrapper_path = f"{venv_bin_path}/python3"
        if not os.path.exists(python3_wrapper_path):
          with open(python3_wrapper_path, "w") as f:
            f.write('#!/usr/bin/env bash\nexec python3 "$@"\n')
          os.chmod(python3_wrapper_path, 0o755)

        pyvenv_cfg_path = f"{venv_folder_path}/pyvenv.cfg"
        if not os.path.exists(pyvenv_cfg_path):
          with open(pyvenv_cfg_path, "w") as f:
            pass

        pyproject_path = f"{cwd}/pyproject.toml"
        uv_pip_install_args = [uv_exec_path, "pip", "install", "--python", python3_wrapper_path, "--link-mode=copy"]

        override_file = None
        if os.path.exists(pyproject_path):
          uv_pip_install_args.extend(["-r", pyproject_path, "--group", "dev"])
        else:
          uv_pip_install_args.append(f"databricks-bundles=={cli_version}")

        try:
          subprocess.run(uv_pip_install_args, check=True, cwd=cwd, stdout=subprocess.DEVNULL, env=env)
        finally:
          if override_file and os.path.exists(override_file):
            os.remove(override_file)
        emit_event("install_dependencies", install_dependencies_start)

        env.pop("DATABRICKS_BUNDLE_RESTRICTED_CODE_EXECUTION", None)
      else:
        env["DATABRICKS_BUNDLE_RESTRICTED_CODE_EXECUTION"] = "1"

      env.pop("DATABRICKS_CLUSTER_ID", None)

      with tempfile.NamedTemporaryFile(mode='w+', dir=tmpdirname, delete=True) as databrickscfg_file:
          profile_name = "workspace_profile"
          databrickscfg_file.write(f"[workspace_profile]\nhost = {workspace_url}")
          databrickscfg_file.flush()
          env["DATABRICKS_CONFIG_FILE"] = databrickscfg_file.name

          cli_command = [cli_exec_path] + shlex.split(args) + ["-p", profile_name]
          tmp_file_path = tmp_file

          run_cli_start = time.monotonic()
          run_cli_phase = "run_cli_genie" if cli_upstream == "genie-code" else "run_cli"
          try:
            _cli_runner_run_cli_command_with_token(
              cli_command,
              cwd,
              tmp_file_path,
              include_stderr_in_error == "true",
              token,
              env,
              cli_upstream=cli_upstream,
            )
          finally:
            emit_event(run_cli_phase, run_cli_start)


try:
  _databricks_cli_runner()
finally:
  del (
    _databricks_cli_runner,
    _cli_runner_create_uv_wrapper,
    _cli_runner_get_venv_path_from_databricks_yml,
    _cli_runner_get_download_url,
    _cli_runner_run_cli_command,
    _cli_runner_run_cli_command_with_token,
  )
None


# COMMAND ----------

def _cli_runner_create_uv_wrapper(uv_exec_path, uv_version, env):
  import os

  uv_wrapper_code = f"""#!/bin/bash -e
if [[ ! -f "{uv_exec_path}" ]]; then
  curl -LsSf https://astral.sh/uv/{uv_version}/install.sh | sh
  exec "$HOME/.local/bin/uv" "$@"
fi
exec "{uv_exec_path}" "$@"
"""
  uv_wrapper_path = "uv"
  with open(uv_wrapper_path, "w") as file:
    file.write(uv_wrapper_code)
  os.chmod(uv_wrapper_path, 0o755)
  env["PATH"] = env.get("PATH", "") + os.pathsep + os.getcwd()


def _cli_runner_get_venv_path_from_databricks_yml(cwd):
  import yaml

  databricks_yml_path = f"{cwd}/databricks.yml"
  with open(databricks_yml_path, "r") as f:
    data = yaml.safe_load(f)
    return data.get("python", {}).get("venv_path", ".venv")


def _cli_runner_run_cli_command(cli_command, cwd, tmp_file_path, include_stderr, env, *, cli_upstream):
  import os
  import subprocess

  stderr_arg = subprocess.PIPE if include_stderr else None
  if tmp_file_path:
    os.makedirs(os.path.dirname(tmp_file_path), exist_ok=True)
    result = subprocess.run(cli_command, cwd=cwd, stdout=subprocess.PIPE, stderr=stderr_arg, text=True, env=env)
    with open(tmp_file_path, 'w') as output_file:
        output_file.write(result.stdout)
        print(result.stdout)
  else:
    result = subprocess.run(cli_command, cwd=cwd, stderr=stderr_arg, text=True, env=env)
  if result.returncode != 0:
    error_msg = f"Command {result.args[:3]} failed with exit code {result.returncode}"
    if include_stderr and result.stderr:
      error_msg += f":\n{result.stderr}"
    raise RuntimeError(error_msg)

  if cli_upstream == "genie-code":
    print(result.stderr)

    try:
      with open("/Workspace/.proc/self/status", "r") as status_file:
        status_file.read()
    except Exception as e:
      print(f"Warning: Failed to read /Workspace/.proc/self/status: {e}")


def _cli_runner_run_cli_command_with_token(cli_command, cwd, tmp_file_path, include_stderr, token, env, *, cli_upstream):
  _cli_runner_run_cli_command(
    cli_command,
    cwd,
    tmp_file_path,
    include_stderr,
    {**env, "DATABRICKS_TOKEN": token},
    cli_upstream=cli_upstream,
  )


def _cli_runner_get_download_url(workspace_url, token, file_api_path, runner_download_error_marker):
  import requests
  import time

  api_create_download_url = f"{workspace_url}/api/2.0/fs/create-download-url"
  headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
  data = {"path": file_api_path}

  max_attempts = 3
  last_error = None
  for attempt in range(max_attempts):
    try:
      response = requests.post(api_create_download_url, headers=headers, json=data)
      if response.status_code == 200:
        return response.json()["url"]
      last_error = f"HTTP {response.status_code} {response.text}"
    except requests.RequestException as e:
      last_error = str(e)
    if attempt < max_attempts - 1:
      time.sleep(2 ** attempt)

  print(f"{runner_download_error_marker}: {file_api_path} {last_error}")
  raise RuntimeError(f"Failed to download {file_api_path}: {last_error}")


def _databricks_cli_runner():
  import hashlib
  import os
  import platform
  import requests
  import shlex
  import shutil
  import subprocess
  import tempfile
  import time
  import zipfile
  from dbruntime.databricks_repl_context import get_context

  cli_version = "1.17.0"
  terraform_version = "1.5.5"
  provider_version = "1.132.0"
  uv_version = "0.6.10"
  cwd = "/Repos/jakkularajeswari266@gmail.com/adb_cicd"
  args = "bundle summary -o json --include-locations --target dev"
  tmp_file = "/Workspace/Users/jakkularajeswari266@gmail.com/.bundle/DABDemo/.output/summary-output-77pudk5adhr.json"
  dev_cli_snippet = ""
  artifact_download_error_prefix = "DABsRunnerError: Failed to download CLI dependency"
  runner_download_error_marker = "DabsRunnerErrorFallback: Downloading error, using fallback URL"
  allow_python_execution = "false"
  enable_yaml_sync = "true"
  enable_direct_engine = "true"
  force_allow_record_deployment_history = "false"
  include_stderr_in_error = "false"
  cli_upstream = "dabs-in-workspace"
  enable_phase_telemetry = "true"
  disable_uv_wrapper = "true"
  enforce_serverless = "true"

  if enforce_serverless == "true" and os.environ.get("IS_SERVERLESS", "").strip().lower() != "true":
    raise RuntimeError("Databricks CLI runner must run on serverless compute")

  setup_runner_environment_start = time.monotonic()
  ctx = get_context()
  token = ctx.apiToken
  workspace_url = f"https://{spark.conf.get('spark.databricks.workspaceUrl')}"
  cpu_arch = "linux_arm64" if platform.machine() == "aarch64" else "linux_amd64"

  env = os.environ.copy()

  def emit_event(phase, start_time):
    if enable_phase_telemetry == "true":
      print(f"\n__RUNNER_PHASE__:{phase}:{(time.monotonic() - start_time) * 1000:.0f}", flush=True)

  def get_download_url(file_api_path):
    return _cli_runner_get_download_url(
      workspace_url, token, file_api_path, runner_download_error_marker
    )

  def download_and_extract(file_url, dest_dir):
    response = requests.get(file_url)
    if response.status_code != 200:
      raise RuntimeError(f"HTTP {response.status_code} while downloading artifact: {response.text}")
    tmp_file = f"{dest_dir}.zip"

    with open(tmp_file, "wb") as zip_file:
        zip_file.write(response.content)
    with zipfile.ZipFile(tmp_file, 'r') as zip_ref:
        zip_ref.extractall(dest_dir)

  def download_artifact_if_not_exists(input_file_name, binary_name, dest_path, tmp_dir):
    if os.path.exists(dest_path):
      return

    try:
      os.makedirs(os.path.dirname(dest_path), exist_ok=True)

      path_prefix = f"/DatabricksInternal/Jobs/ArtifactRepository/public/databricks-cli-binaries/{cli_version}"
      download_url = get_download_url(f"{path_prefix}/{input_file_name}.zip")

      tmp_archive_dir = f"{tmp_dir}/{input_file_name}"
      download_and_extract(download_url, tmp_archive_dir)

      src_path = f"{tmp_archive_dir}/{binary_name}"
      if not os.path.exists(src_path):
        available = ", ".join(os.listdir(tmp_archive_dir)) if os.path.isdir(tmp_archive_dir) else "(missing archive directory)"
        raise RuntimeError(f"Expected binary '{binary_name}' not found in archive contents: {available}")

      shutil.move(src_path, dest_path)
      os.chmod(dest_path, 0o755)
    except Exception as e:
      error_msg = f"{artifact_download_error_prefix} {input_file_name}/{binary_name} CLI v{cli_version}, TF v{terraform_version}, TF provider v{provider_version}: {str(e)}"
      raise RuntimeError(f"{error_msg}")

  tmp_base = os.path.expanduser("~/tmp")
  if not os.path.exists(tmp_base):
    os.makedirs(tmp_base)
  os.chmod(tmp_base, 0o700)

  with tempfile.TemporaryDirectory(dir=tmp_base) as tmpdirname:
      emit_event("setup_runner_environment", setup_runner_environment_start)

      download_artifacts_start = time.monotonic()
      cli_exec_path = f"{tmp_base}/databricks_{cli_version}_{cpu_arch}"
      download_artifact_if_not_exists(
        f"databricks_cli_{cpu_arch}",
        "databricks",
        cli_exec_path,
        tmpdirname
      )

      terraform_base = f"{tmp_base}/terraform"
      terraform_exec_path = f"{terraform_base}/terraform_{terraform_version}_{cpu_arch}"
      download_artifact_if_not_exists(
        f"terraform_{cpu_arch}",
        "terraform",
        terraform_exec_path,
        tmpdirname
      )

      provider_mirror_base = f"{terraform_base}/providers"
      provider_mirror_dir = f"{provider_mirror_base}/registry.terraform.io/databricks/databricks/{provider_version}/{cpu_arch}"
      os.makedirs(provider_mirror_dir, exist_ok=True)
      provider_exec_path = f"{provider_mirror_dir}/terraform-provider-databricks_v{provider_version}"
      download_artifact_if_not_exists(
        f"terraform_provider_{cpu_arch}",
        f"terraform-provider-databricks_v{provider_version}",
        provider_exec_path,
        tmpdirname
      )

      if disable_uv_wrapper != "true":
        uv_base = f"{tmp_base}/uv"
        uv_exec_path = f"{uv_base}/uv_{uv_version}_{cpu_arch}"
        try:
            download_artifact_if_not_exists(
              f"uv_{cpu_arch}",
              "uv",
              uv_exec_path,
              tmpdirname
            )
        except Exception:
            pass
      else:
        uv_exec_path = "uv"
      emit_event("download_artifacts", download_artifacts_start)

      setup_cli_environment_start = time.monotonic()
      if disable_uv_wrapper != "true":
        _cli_runner_create_uv_wrapper(uv_exec_path, uv_version, env)

      terraform_cli_config_path = f"{terraform_base}/terraform-cli-config"
      if not os.path.exists(terraform_cli_config_path):
        terraform_cli_config = f'''provider_installation {{
  filesystem_mirror {{
    path    = "{provider_mirror_base}"
    include = ["registry.terraform.io/databricks/databricks"]
  }}
  direct {{
    exclude = ["registry.terraform.io/databricks/databricks"]
  }}
}}
disable_checkpoint = true
disable_checkpoint_signature = true
'''

        with open(terraform_cli_config_path, 'w') as f:
          f.write(terraform_cli_config)

      cwd = os.path.normpath(f"/Workspace{cwd}")

      env["DATABRICKS_TF_VERSION"] = terraform_version
      env["DATABRICKS_TF_EXEC_PATH"] = terraform_exec_path
      env["DATABRICKS_TF_PROVIDER_VERSION"] = provider_version
      env["DATABRICKS_TF_CLI_CONFIG_FILE"] = terraform_cli_config_path
      env["TF_CLI_CONFIG_FILE"] = terraform_cli_config_path

      if dev_cli_snippet:
        cli_path = cli_exec_path
        exec(dev_cli_snippet, globals(), locals())

      env["DATABRICKS_BUNDLE_TMP"] = f"{tmp_base}/bundle-2-{hashlib.sha256(cwd.encode()).hexdigest()}"
      env["DATABRICKS_CLI_UPSTREAM"] = cli_upstream

      if enable_yaml_sync == "true":
        env["DATABRICKS_BUNDLE_ENABLE_EXPERIMENTAL_YAML_SYNC"] = "1"
      else:
        env.pop("DATABRICKS_BUNDLE_ENABLE_EXPERIMENTAL_YAML_SYNC", None)

      if enable_direct_engine == "true":
        env["DATABRICKS_BUNDLE_ENGINE"] = "direct"
      else:
        env.pop("DATABRICKS_BUNDLE_ENGINE", None)

      if force_allow_record_deployment_history == "true":
        env["DATABRICKS_BUNDLE_FORCE_ALLOW_RECORD_DEPLOYMENT_HISTORY"] = "true"
        env["DATABRICKS_BUNDLE_INIT_DEPLOYMENT_HISTORY"] = "true"
      else:
        env.pop("DATABRICKS_BUNDLE_FORCE_ALLOW_RECORD_DEPLOYMENT_HISTORY", None)
        env.pop("DATABRICKS_BUNDLE_INIT_DEPLOYMENT_HISTORY", None)

      emit_event("setup_cli_environment", setup_cli_environment_start)

      if allow_python_execution == "true":
        install_dependencies_start = time.monotonic()
        venv_path = _cli_runner_get_venv_path_from_databricks_yml(cwd)
        venv_folder_path = os.path.normpath(f"{cwd}/{venv_path}")
        venv_bin_path = f"{venv_folder_path}/bin"

        os.makedirs(venv_bin_path, exist_ok=True)

        python3_wrapper_path = f"{venv_bin_path}/python3"
        if not os.path.exists(python3_wrapper_path):
          with open(python3_wrapper_path, "w") as f:
            f.write('#!/usr/bin/env bash\nexec python3 "$@"\n')
          os.chmod(python3_wrapper_path, 0o755)

        pyvenv_cfg_path = f"{venv_folder_path}/pyvenv.cfg"
        if not os.path.exists(pyvenv_cfg_path):
          with open(pyvenv_cfg_path, "w") as f:
            pass

        pyproject_path = f"{cwd}/pyproject.toml"
        uv_pip_install_args = [uv_exec_path, "pip", "install", "--python", python3_wrapper_path, "--link-mode=copy"]

        override_file = None
        if os.path.exists(pyproject_path):
          uv_pip_install_args.extend(["-r", pyproject_path, "--group", "dev"])
        else:
          uv_pip_install_args.append(f"databricks-bundles=={cli_version}")

        try:
          subprocess.run(uv_pip_install_args, check=True, cwd=cwd, stdout=subprocess.DEVNULL, env=env)
        finally:
          if override_file and os.path.exists(override_file):
            os.remove(override_file)
        emit_event("install_dependencies", install_dependencies_start)

        env.pop("DATABRICKS_BUNDLE_RESTRICTED_CODE_EXECUTION", None)
      else:
        env["DATABRICKS_BUNDLE_RESTRICTED_CODE_EXECUTION"] = "1"

      env.pop("DATABRICKS_CLUSTER_ID", None)

      with tempfile.NamedTemporaryFile(mode='w+', dir=tmpdirname, delete=True) as databrickscfg_file:
          profile_name = "workspace_profile"
          databrickscfg_file.write(f"[workspace_profile]\nhost = {workspace_url}")
          databrickscfg_file.flush()
          env["DATABRICKS_CONFIG_FILE"] = databrickscfg_file.name

          cli_command = [cli_exec_path] + shlex.split(args) + ["-p", profile_name]
          tmp_file_path = tmp_file

          run_cli_start = time.monotonic()
          run_cli_phase = "run_cli_genie" if cli_upstream == "genie-code" else "run_cli"
          try:
            _cli_runner_run_cli_command_with_token(
              cli_command,
              cwd,
              tmp_file_path,
              include_stderr_in_error == "true",
              token,
              env,
              cli_upstream=cli_upstream,
            )
          finally:
            emit_event(run_cli_phase, run_cli_start)


try:
  _databricks_cli_runner()
finally:
  del (
    _databricks_cli_runner,
    _cli_runner_create_uv_wrapper,
    _cli_runner_get_venv_path_from_databricks_yml,
    _cli_runner_get_download_url,
    _cli_runner_run_cli_command,
    _cli_runner_run_cli_command_with_token,
  )
None
