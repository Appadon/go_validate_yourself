import json
import os
import tempfile
import unittest
from pathlib import Path
from typing import Any, Dict, Optional
from unittest.mock import patch

from gvy_sdk import Gvy


class RecordingGvy(Gvy):
	def __init__(self, response: Dict[str, Any]) -> None:
		super().__init__()
		self.response = response
		self.requests: list[Dict[str, Any]] = []

	def _request(
		self,
		method: str,
		path: str,
		payload: Optional[Dict[str, Any]] = None,
		timeout: Optional[float] = None,
	) -> Dict[str, Any]:
		self.requests.append(
			{
				"method": method,
				"path": path,
				"payload": payload,
				"timeout": timeout,
			}
		)
		return self.response


class TestSdkClient(unittest.TestCase):
	def test_start_server_uses_working_dir_for_server_root(self) -> None:
		class FakeProcess:
			def poll(self) -> None:
				return None

		with tempfile.TemporaryDirectory() as temp_dir:
			root = Path(temp_dir)
			binary_dir = root / "bin"
			working_dir = root / "project"
			binary_dir.mkdir()
			working_dir.mkdir()
			binary_path = binary_dir / "gvy"
			binary_path.write_text("#!/bin/sh\n", encoding="utf-8")

			client = Gvy(binary_path=str(binary_path), working_dir=working_dir)

			with patch("gvy_sdk.client.subprocess.Popen", return_value=FakeProcess()) as popen:
				client.start_server()

		_, kwargs = popen.call_args
		self.assertEqual(kwargs["cwd"], str(working_dir.resolve()))

	def test_relative_binary_path_resolves_from_working_dir(self) -> None:
		with tempfile.TemporaryDirectory() as temp_dir:
			root = Path(temp_dir)
			working_dir = root / "project"
			working_dir.mkdir()
			binary_path = working_dir / "gvy"
			binary_path.write_text("#!/bin/sh\n", encoding="utf-8")

			client = Gvy(binary_path="./gvy", working_dir=working_dir)

			self.assertEqual(client._resolve_binary_path(), binary_path.resolve())

	def test_default_working_dir_is_caller_cwd(self) -> None:
		previous_cwd = os.getcwd()
		with tempfile.TemporaryDirectory() as temp_dir:
			try:
				os.chdir(temp_dir)
				client = Gvy()

				self.assertEqual(client._resolve_working_dir(), Path(temp_dir).resolve())
			finally:
				os.chdir(previous_cwd)

	def test_run_config_sends_config_payload(self) -> None:
		client = RecordingGvy({"ok": True})
		config = {"mode": "split", "inputs": {"main_csv": "input.csv"}}

		response = client.run_config(config)

		self.assertEqual(response, {"ok": True})
		self.assertEqual(
			client.requests,
			[
				{
					"method": "POST",
					"path": "/api/runs/config",
					"payload": config,
					"timeout": None,
				}
			],
		)

	def test_resolve_config_loads_config_path(self) -> None:
		client = RecordingGvy({"ok": True, "resolved_config": {"mode": "validate"}})
		config = {
			"mode": "validate",
			"pipeline": {"phases": ["validate"]},
			"inputs": {"schema": "schema.json", "validate_dir": "split"},
		}

		with tempfile.TemporaryDirectory() as temp_dir:
			config_path = f"{temp_dir}/gvy.config.json"
			with open(config_path, "w", encoding="utf-8") as handle:
				json.dump(config, handle)

			response = client.resolve_config(config_path=config_path)

		self.assertEqual(response["resolved_config"]["mode"], "validate")
		self.assertEqual(client.requests[0]["path"], "/api/config/resolve")
		self.assertEqual(client.requests[0]["payload"], config)

	def test_run_manager_helpers_call_run_endpoints(self) -> None:
		client = RecordingGvy({"ok": True})

		client.list_runs()
		client.get_run("run-1")
		client.get_run_result("run-1")

		self.assertEqual(
			[request["path"] for request in client.requests],
			["/api/runs", "/api/runs/run-1", "/api/runs/run-1/result"],
		)

	def test_run_manager_helpers_quote_run_id(self) -> None:
		client = RecordingGvy({"ok": True})

		client.get_run("run/1")

		self.assertEqual(client.requests[0]["path"], "/api/runs/run%2F1")

	def test_run_validate_auto_uses_explicit_config_and_legacy_response(self) -> None:
		client = RecordingGvy(
			{
				"ok": True,
				"run": {"id": "run-1"},
				"resolved_config": {
					"inputs": {
						"main_csv": "/tmp/input.csv",
						"schema": "/tmp/schema.json",
					},
					"outputs": {
						"split_dir": "/tmp/split",
						"success_dir": "/tmp/success",
						"error_dir": "/tmp/errors",
						"batch_export_dir": "/tmp/batch_export",
					},
					"batch": {"input_dir": "/tmp/success"},
				},
				"result": {
					"split_primary_key": "Record ID",
					"split_reused": False,
					"split_summary": {"files_written": 2},
					"validation_dir": {"file_count": 2},
					"batch_summary": {"files_written": 1},
				},
			}
		)

		response = client.run_validate_auto(
			input_csv="/tmp/input.csv",
			schema_path="/tmp/schema.json",
			threads=12,
		)

		self.assertEqual(client.requests[0]["path"], "/api/runs/config")
		self.assertEqual(
			client.requests[0]["payload"],
			{
				"mode": "auto",
				"pipeline": {"phases": ["split", "validate", "batch"]},
				"inputs": {
					"main_csv": "/tmp/input.csv",
					"schema": "/tmp/schema.json",
				},
				"runtime": {"workers": 12},
			},
		)
		self.assertEqual(response["mode"], "auto")
		self.assertEqual(response["outputs"]["success_dir"], "/tmp/success")
		self.assertEqual(response["result"]["main_input_csv"], "/tmp/input.csv")
		self.assertEqual(response["result"]["validation"], {"file_count": 2})
		self.assertEqual(response["run"], {"id": "run-1"})


if __name__ == "__main__":
	unittest.main()
