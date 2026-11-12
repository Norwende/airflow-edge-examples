"""Auto-test de ci/promote.py (stdlib uniquement) : rendu/relecture du version.yaml et parcours
complet contre un faux GitLab (http.server local).

    python -m unittest discover -s ci -p "test_*.py" -v
"""

from __future__ import annotations

import contextlib
import io
import json
import os
import sys
import threading
import unittest
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, ClassVar

os.environ.setdefault("PROMOTE_POLL_INTERVAL", "0.01")
os.environ.setdefault("PROMOTE_HEAD_PIPELINE_TIMEOUT", "2")
os.environ.setdefault("PROMOTE_MERGE_TIMEOUT", "2")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import promote  # noqa: E402

SAMPLE = (
    (
        "# charts/apps/notes-api/envs/dev/version.yaml"
        "  (écrit par la CI de gitops/notes-api, jamais à la main)\n"
    )
    + """\
image:
  repository: registry.127-0-0-1.sslip.io/gitops/notes-api/develop/notes-api
  tag: "1a2b3c4d"
source:                       # traçabilité, informatif
  commit: 1a2b3c4d5e6f7890aaaabbbbccccddddeeeeffff
  ref: develop
  pipeline: http://gitlab.127-0-0-1.sslip.io/gitops/notes-api/-/pipelines/123
"""
)

FAKE_TOKEN = "glpat-" + "test-token-xyz"  # jeton factice du faux serveur
REPO = "registry.127-0-0-1.sslip.io/gitops/notes-api/develop/notes-api"
FILE = "charts/apps/notes-api/envs/dev/version.yaml"


class RenderTest(unittest.TestCase):
    def test_render_matches_contract_sample(self) -> None:
        rendered = promote.render_version_yaml(
            "dev",
            REPO,
            "1a2b3c4d",
            "1a2b3c4d5e6f7890aaaabbbbccccddddeeeeffff",
            "develop",
            "http://gitlab.127-0-0-1.sslip.io/gitops/notes-api/-/pipelines/123",
        )
        self.assertEqual(rendered, SAMPLE)

    def test_parse_back(self) -> None:
        parsed = promote.parse_version_yaml(SAMPLE)
        self.assertEqual(list(parsed), ["image", "source"])
        self.assertEqual(list(parsed["image"]), ["repository", "tag"])
        self.assertEqual(list(parsed["source"]), ["commit", "ref", "pipeline"])
        self.assertEqual(parsed["image"]["tag"], "1a2b3c4d")
        self.assertEqual(parsed["source"]["ref"], "develop")

    def test_ambiguous_scalars_are_quoted(self) -> None:
        self.assertEqual(promote.yaml_scalar("12345678"), '"12345678"')
        self.assertEqual(promote.yaml_scalar("1234e567"), '"1234e567"')
        self.assertEqual(promote.yaml_scalar(""), '""')
        self.assertEqual(promote.yaml_scalar("v1.2.3"), "v1.2.3")
        self.assertEqual(promote.yaml_scalar("abc123"), "abc123")
        rendered = promote.render_version_yaml("prod", REPO, "1.2.3", "12345678", "v1.2.3", "")
        parsed = promote.parse_version_yaml(rendered)
        self.assertEqual(parsed["image"]["tag"], "1.2.3")
        self.assertEqual(parsed["source"]["commit"], "12345678")
        self.assertEqual(parsed["source"]["pipeline"], "")

    def test_pyyaml_agrees_if_available(self) -> None:
        try:
            import yaml  # type: ignore[import-untyped]
        except ImportError:
            self.skipTest("PyYAML absent")
        data = yaml.safe_load(SAMPLE)
        self.assertEqual(data["image"]["tag"], "1a2b3c4d")
        odd = yaml.safe_load(promote.render_version_yaml("prod", REPO, "1.0", "1234e5", "v1", ""))
        self.assertEqual(odd["image"]["tag"], "1.0")
        self.assertEqual(odd["source"]["commit"], "1234e5")


# --------------------------------------------------------------------------- faux GitLab
class FakeGitLab:
    """État minimal d'un projet manifests : fichiers par branche, MR, appels reçus."""

    def __init__(self) -> None:
        self.files: dict[str, dict[str, str]] = {"main": {FILE: SAMPLE}}
        self.mrs: list[dict[str, Any]] = []
        self.calls: list[tuple[str, str]] = []
        self.commits: list[dict[str, Any]] = []
        self.tokens: set[str] = set()
        self.pipeline_status = "running"
        self.merge_refusals = 1  # premier PUT /merge refusé (405) pour tester le ré-essai


class Handler(BaseHTTPRequestHandler):
    state: ClassVar[FakeGitLab]

    def log_message(self, format: str, *args: Any) -> None:
        pass

    def _send(self, status: int, payload: Any, raw: bool = False) -> None:
        body = payload.encode() if raw else json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "text/plain" if raw else "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _body(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length") or 0)
        return json.loads(self.rfile.read(length)) if length else {}

    def _route(self, method: str) -> None:
        st = self.state
        st.tokens.add(self.headers.get("PRIVATE-TOKEN", ""))
        parsed = urllib.parse.urlsplit(self.path)
        path = parsed.path  # %2F encore encodés : on découpe sur les vrais "/"
        query = dict(urllib.parse.parse_qsl(parsed.query))
        st.calls.append((method, path))
        prefix = "/api/v4/projects/gitops%2Fmanifests"
        if not path.startswith(prefix):
            return self._send(404, {"message": "404 Project Not Found"})
        rest = path[len(prefix) :]
        unq = urllib.parse.unquote
        parts = rest.split("/")
        body = self._body() if method in {"POST", "PUT"} else {}

        if method == "GET" and rest.startswith("/repository/files/") and rest.endswith("/raw"):
            file_path = unq(parts[3])
            content = st.files.get(query.get("ref", ""), {}).get(file_path)
            if content is None:
                return self._send(404, {"message": "404 File Not Found"})
            return self._send(200, content, raw=True)
        if rest.startswith("/repository/branches"):
            if method == "GET" and len(parts) == 4:
                if unq(parts[3]) in st.files:
                    return self._send(200, {"name": unq(parts[3])})
                return self._send(404, {"message": "404 Branch Not Found"})
            if method == "POST":
                st.files[query["branch"]] = dict(st.files[query["ref"]])
                return self._send(201, {"name": query["branch"]})
            if method == "DELETE":
                st.files.pop(unq(parts[3]), None)
                return self._send(204, "", raw=True)
        if method == "POST" and rest == "/repository/commits":
            st.commits.append(body)
            for action in body["actions"]:
                st.files[body["branch"]][action["file_path"]] = action["content"]
            return self._send(201, {"id": "deadbeef"})
        if rest == "/merge_requests" and method == "GET":
            found = [
                m
                for m in st.mrs
                if m["state"] == query.get("state", m["state"])
                and m["source_branch"] == query.get("source_branch", m["source_branch"])
            ]
            return self._send(200, found)
        if rest == "/merge_requests" and method == "POST":
            mr = {
                "iid": len(st.mrs) + 1,
                "state": "opened",
                "web_url": f"http://fake/mr/{len(st.mrs) + 1}",
                "head_pipeline": None,
                "sha": "abc",
                **body,
            }
            st.mrs.append(mr)
            return self._send(201, mr)
        if rest.startswith("/merge_requests/"):
            mr = st.mrs[int(parts[2]) - 1]
            if method == "GET":
                # la pipeline de MR « apparaît » au 2e GET
                if mr["head_pipeline"] is None and mr.get("_gets", 0) >= 1:
                    mr["head_pipeline"] = {"id": 1, "status": st.pipeline_status}
                mr["_gets"] = mr.get("_gets", 0) + 1
                return self._send(200, mr)
            if method == "PUT" and rest.endswith("/merge"):
                if st.merge_refusals > 0:
                    st.merge_refusals -= 1
                    return self._send(405, {"message": "405 Method Not Allowed"})
                if body.get("merge_when_pipeline_succeeds"):
                    mr["merge_when_pipeline_succeeds"] = True
                else:
                    mr["state"] = "merged"
                return self._send(200, mr)
            if method == "PUT":
                if body.get("state_event") == "close":
                    mr["state"] = "closed"
                mr.update({k: v for k, v in body.items() if k != "state_event"})
                return self._send(200, mr)
            if method == "POST" and rest.endswith("/notes"):
                return self._send(201, {"id": 1})
        return self._send(404, {"message": f"route inconnue {method} {rest}"})

    def do_GET(self) -> None:  # noqa: N802
        self._route("GET")

    def do_POST(self) -> None:  # noqa: N802
        self._route("POST")

    def do_PUT(self) -> None:  # noqa: N802
        self._route("PUT")

    def do_DELETE(self) -> None:  # noqa: N802
        self._route("DELETE")


class FlowTest(unittest.TestCase):
    def setUp(self) -> None:
        self.state = FakeGitLab()
        Handler.state = self.state
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.api = f"http://127.0.0.1:{self.server.server_address[1]}/api/v4"
        os.environ["MANIFESTS_TOKEN"] = FAKE_TOKEN

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()

    def _run(self, *extra: str, tag: str = "9f8e7d6c") -> str:
        argv = [
            "--env", "dev", "--repository", REPO, "--tag", tag, "--ref", "develop",
            "--commit", "9f8e7d6c" + "0" * 32, "--pipeline-url", "http://gl/p/1",
            "--api-url", self.api, "--project", "gitops/manifests", *extra,
        ]  # fmt: skip
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            self.assertEqual(promote.main(argv), 0)
        return out.getvalue()

    def test_full_flow_with_auto_merge_then_idempotent(self) -> None:
        out = self._run("--auto-merge")
        branch = "promote/notes-api-dev-9f8e7d6c"
        self.assertNotIn(FAKE_TOKEN, out)
        self.assertEqual(self.state.tokens, {FAKE_TOKEN})
        self.assertEqual(len(self.state.commits), 1)
        self.assertEqual(self.state.commits[0]["actions"][0]["action"], "update")
        new = promote.parse_version_yaml(self.state.files[branch][FILE])
        self.assertEqual(new["image"]["tag"], "9f8e7d6c")
        mr = self.state.mrs[0]
        self.assertEqual(mr["title"], "chore(notes-api): dev -> 9f8e7d6c")
        self.assertEqual(mr["labels"], "promotion,env::dev")
        self.assertTrue(mr["remove_source_branch"])
        self.assertTrue(mr.get("merge_when_pipeline_succeeds"))
        self.assertIn("merge automatique", out)

        # 2e exécution, MR toujours ouverte : ni nouveau commit ni nouvelle MR.
        self.state.merge_refusals = 0
        self._run("--auto-merge")
        self.assertEqual(len(self.state.commits), 1)
        self.assertEqual(len(self.state.mrs), 1)

        # Après merge (main à jour) : rien à faire du tout.
        self.state.files["main"][FILE] = self.state.files[branch][FILE]
        before = len(self.state.calls)
        out = self._run("--auto-merge")
        self.assertIn("rien à faire", out)
        self.assertEqual(len(self.state.calls) - before, 1)  # une seule lecture

    def test_prod_no_auto_merge_and_supersede(self) -> None:
        self._run(tag="aaaa1111")
        self._run(tag="bbbb2222")
        self.assertEqual(self.state.mrs[0]["state"], "closed")
        self.assertEqual(self.state.mrs[1]["state"], "opened")
        self.assertNotIn("promote/notes-api-dev-aaaa1111", self.state.files)
        self.assertFalse(any(path.endswith("/merge") for _, path in self.state.calls))

    def test_dry_run_writes_nothing(self) -> None:
        out = self._run("--dry-run")
        self.assertIn('tag: "9f8e7d6c"', out)
        self.assertTrue(all(method == "GET" for method, _ in self.state.calls))


if __name__ == "__main__":
    unittest.main()
