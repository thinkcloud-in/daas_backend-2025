import unittest

from service.temporalResource.activity.activities_kubernetes_deploy import (
    _extract_archive_spec,
    _looks_like_docker_requirement,
    _resolve_chart_ref,
    _resolve_node_port,
    _should_run_install_script,
)


class ExtractArchiveSpecTests(unittest.TestCase):
    def test_tar_zst_is_supported(self):
        spec = _extract_archive_spec("harbor-offline-installer-v2.10.0.tar.zst")
        self.assertIsNotNone(spec)
        self.assertEqual(spec["kind"], "tar.zst")
        self.assertEqual(spec["tool"], "zstd")

    def test_tar_gz_is_supported(self):
        spec = _extract_archive_spec("harbor-offline-installer-v2.10.0.tar.gz")
        self.assertIsNotNone(spec)
        self.assertEqual(spec["kind"], "tar.gz")
        self.assertIsNone(spec["tool"])

    def test_tgz_is_supported(self):
        spec = _extract_archive_spec("harbor-offline-installer-v2.15.2.tgz")
        self.assertIsNotNone(spec)
        self.assertEqual(spec["kind"], "tar.gz")
        self.assertIsNone(spec["tool"])

    def test_docker_requirement_is_detected(self):
        detail = "[Step 0]: checking if docker is installed ...\n✖ Need to install docker(20.10.10+) first"
        self.assertTrue(_looks_like_docker_requirement(detail))

    def test_install_script_is_skipped_when_k8s_assets_exist_without_docker(self):
        self.assertFalse(_should_run_install_script("/tmp/install.sh", "/tmp/chart/Chart.yaml", 0, False))
        self.assertFalse(_should_run_install_script("/tmp/install.sh", "", 1, False))

    def test_chart_ref_is_resolved_from_values_yaml(self):
        ref = _resolve_chart_ref("", "/tmp/harbor/values.yaml", "")
        self.assertIsNotNone(ref)
        self.assertEqual(ref["kind"], "dir")
        self.assertEqual(ref["path"], "/tmp/harbor")

    def test_chart_ref_is_resolved_from_chart_archive(self):
        ref = _resolve_chart_ref("", "", "/tmp/harbor-chart.tgz")
        self.assertIsNotNone(ref)
        self.assertEqual(ref["kind"], "archive")
        self.assertEqual(ref["path"], "/tmp/harbor-chart.tgz")

    def test_node_port_falls_back_to_valid_kubernetes_range(self):
        self.assertEqual(_resolve_node_port(80), 30080)
        self.assertEqual(_resolve_node_port(30080), 30080)
        self.assertEqual(_resolve_node_port(32767), 32767)


if __name__ == "__main__":
    unittest.main()
