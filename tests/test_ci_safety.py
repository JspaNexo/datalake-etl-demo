import copy
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from scripts.ci.prepare_release import prepare_release
from scripts.ci.run_integration import run_integration, validate_compose_config, validate_project_name, validate_seq_events


class CiSafetyTests(unittest.TestCase):
    def setUp(self):
        self.project = "datalake-ci-test123"
        self.config = {
            "name": self.project, "services": {"etl": {"image": "datalake-etl-demo:test"}},
            "volumes": {"postgres": {"name": self.project + "_postgres"}},
            "networks": {"default": {"name": self.project + "_default"}},
        }

    def test_demo_and_invalid_project_names_are_rejected(self):
        for name in ("datalake-demo", "datalake-ci-", "datalake-ci-../demo", "datalake-ci-TEST"):
            with self.subTest(name=name), self.assertRaises(ValueError):
                validate_project_name(name)

    def test_isolated_configuration_is_accepted(self):
        validate_compose_config(self.config, self.project)

    def test_external_or_demo_resources_are_rejected(self):
        for category in ("volumes", "networks"):
            for resource in ({"name": "datalake-demo_postgres_data"}, {"external": True, "name": self.project + "_shared"}):
                with self.subTest(category=category, resource=resource):
                    config = copy.deepcopy(self.config)
                    config[category] = {"bad": resource}
                    with self.assertRaises(ValueError):
                        validate_compose_config(config, self.project)

    def test_host_ports_and_bind_mounts_are_rejected(self):
        for field, value in (("ports", [{"published": "3000", "target": 3000}]),
                             ("volumes", [{"type": "bind", "source": "/app", "target": "/app"}])):
            with self.subTest(field=field):
                config = copy.deepcopy(self.config)
                config["services"]["etl"][field] = value
                with self.assertRaises(ValueError):
                    validate_compose_config(config, self.project)

    def test_incomplete_logs_do_not_count_as_successful_integration(self):
        with self.assertRaises(ValueError):
            validate_seq_events([])
        events = [{"Application": "datalake_demo_ci", "CorrelationId": run, "Stage": stage,
                   "ValidRows": 8, "RejectedRows": 6, "RevenueBs": "356.00"}
                  for run in ("run-1", "run-2") for stage in ("bronze", "silver", "gold", "sql")]
        self.assertEqual(validate_seq_events(events)["stages_per_run"], 4)
        events[0]["@l"] = "Error"
        with self.assertRaises(ValueError):
            validate_seq_events(events)

    def test_existing_project_is_not_started_or_cleaned_up(self):
        result = SimpleNamespace(stdout=json.dumps(self.config), stderr="", returncode=0)
        with tempfile.TemporaryDirectory() as directory, \
                patch("scripts.ci.run_integration.execute", return_value=result) as execute, \
                patch("scripts.ci.run_integration.assert_project_is_unused", side_effect=ValueError("Ya existe")):
            with self.assertRaisesRegex(ValueError, "Ya existe"):
                run_integration("datalake-etl-demo:test", self.project, Path(directory))
        self.assertEqual(execute.call_count, 1)
        self.assertIn("config", execute.call_args.args[0])

    def test_startup_failure_still_cleans_owned_resources_and_marks_failure(self):
        commands = []

        def execute(command, env, **kwargs):
            commands.append(command)
            if "config" in command:
                return SimpleNamespace(stdout=json.dumps(self.config), stderr="", returncode=0)
            if "inspect" in command:
                return SimpleNamespace(stdout="sha256:test-image", stderr="", returncode=0)
            if "up" in command:
                raise RuntimeError("Fallo de arranque simulado")
            return SimpleNamespace(stdout="", stderr="", returncode=0)

        with tempfile.TemporaryDirectory() as directory, \
                patch("scripts.ci.run_integration.execute", side_effect=execute), \
                patch("scripts.ci.run_integration.assert_project_is_unused"):
            with self.assertRaisesRegex(RuntimeError, "Fallo de arranque simulado"):
                run_integration("datalake-etl-demo:test", self.project, Path(directory))
            report = json.loads((Path(directory) / "summary.json").read_text(encoding="utf-8"))
        self.assertEqual(report["status"], "failed")
        self.assertTrue(any("down" in command and "--volumes" in command for command in commands))

    def test_release_rejects_an_image_different_from_the_tested_one(self):
        def execute(command, **kwargs):
            output = json.dumps({"Id": "sha256:other-image", "RepoDigests": []}) if command[0] == "docker" else "abcdef"
            return SimpleNamespace(stdout=output, stderr="", returncode=0)

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            summary = root / "artifacts" / "integration" / "summary.json"
            summary.parent.mkdir(parents=True)
            summary.write_text(json.dumps({"status": "passed", "image_id": "sha256:tested-image"}), encoding="utf-8")
            with patch("scripts.ci.prepare_release.ROOT", root), patch("scripts.ci.prepare_release.subprocess.run", side_effect=execute):
                with self.assertRaisesRegex(ValueError, "exactamente la que paso"):
                    prepare_release("datalake-etl-demo:test", root / "release")
            self.assertFalse((root / "release").exists())
