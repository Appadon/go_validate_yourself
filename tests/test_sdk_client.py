import json
import tempfile
import unittest
from typing import Any, Dict, Optional

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
