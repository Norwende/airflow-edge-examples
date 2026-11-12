#!/usr/bin/env python3
"""Promotion d'une image notes-api dans le dépôt gitops/manifests (CONTRACT.md §5).

Tout passe par l'API GitLab (aucun clone), bibliothèque standard uniquement :

1. lit ``charts/apps/notes-api/envs/<env>/version.yaml`` sur ``main`` ; si ``image.tag`` (et
   ``image.repository``) sont déjà les bons, il n'y a rien à faire (idempotent) ;
2. crée (ou réutilise) la branche ``promote/notes-api-<env>-<tag>`` depuis ``main`` ;
3. commit ``chore(notes-api): <env> -> <tag>`` qui réécrit ``version.yaml`` en entier (§4) ;
4. ouvre (ou met à jour) la MR vers ``main`` : labels ``promotion`` + ``env::<env>``,
   ``remove_source_branch``, description avec image, commit et pipeline ; les MR de promotion
   plus anciennes du même environnement, encore ouvertes, sont fermées (remplacées) ;
5. ``--auto-merge`` (dev) : attend la pipeline de la MR puis demande le merge automatique quand
   elle réussit (merge direct si elle a déjà réussi). Sans ce drapeau (prod), le lead merge.

Le token (``MANIFESTS_TOKEN``) part dans l'en-tête ``PRIVATE-TOKEN``, jamais dans une URL ni dans
les logs. ``--dry-run`` affiche le plan et le fichier rendu sans rien écrire.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any

VERSION_FILE = "charts/apps/notes-api/envs/{env}/version.yaml"
TARGET_BRANCH = "main"
BRANCH_PREFIX = "promote/notes-api-{env}-"
LABEL_PROMOTION = "promotion"

# Délais (surchargeables pour les tests).
HEAD_PIPELINE_TIMEOUT_S = float(os.environ.get("PROMOTE_HEAD_PIPELINE_TIMEOUT", "180"))
MERGE_RETRY_TIMEOUT_S = float(os.environ.get("PROMOTE_MERGE_TIMEOUT", "180"))
POLL_INTERVAL_S = float(os.environ.get("PROMOTE_POLL_INTERVAL", "5"))
HTTP_TIMEOUT_S = 30.0

# Même forme que l'exemple de CONTRACT.md §4 (ordre des clés : image.repository, image.tag,
# source.commit, source.ref, source.pipeline). `tag` toujours entre guillemets.
TEMPLATE = """\
# {path}  (écrit par la CI de gitops/notes-api, jamais à la main)
image:
  repository: {repository}
  tag: "{tag}"
source:                       # traçabilité, informatif
  commit: {commit}
  ref: {ref}
  pipeline: {pipeline}
"""

TAG_RE = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9._-]{0,127}$")
REPOSITORY_RE = re.compile(r"^[a-z0-9][a-z0-9._:/-]*[a-z0-9]$")
ENV_RE = re.compile(r"^[a-z][a-z0-9-]*$")

# Scalaires YAML qu'un parseur interpréterait autrement qu'en chaîne (nombres, booléens, null...).
_YAML_NUMBER_RE = re.compile(
    r"^[-+]?(\.inf|\.nan|0x[0-9a-fA-F_]+|0o?[0-7_]+|[0-9][0-9_]*(:[0-5]?[0-9])+(\.[0-9_]*)?"
    r"|([0-9][0-9_]*)?\.?[0-9_]*([eE][-+]?[0-9]+)?)$",
    re.IGNORECASE,
)
_YAML_SPECIAL_WORDS = {"y", "yes", "n", "no", "true", "false", "on", "off", "null", "~"}


def log(message: str) -> None:
    print(f"[promote] {message}", flush=True)


class PromoteError(RuntimeError):
    """Erreur fatale de promotion (message destiné au log du job)."""


# --------------------------------------------------------------------------- rendu / lecture YAML
def yaml_scalar(value: str) -> str:
    """Scalaire YAML : brut quand c'est sans ambiguïté, sinon entre guillemets."""
    plain_ok = (
        value != ""
        and value.strip() == value
        and value.lower() not in _YAML_SPECIAL_WORDS
        and not _YAML_NUMBER_RE.match(value)
        and value[0] not in "!&*-?{}[],#|>@`\"'%:"
        and ": " not in value
        and " #" not in value
        and "\n" not in value
    )
    return value if plain_ok else json.dumps(value)


def render_version_yaml(
    env: str, repository: str, tag: str, commit: str, ref: str, pipeline: str
) -> str:
    return TEMPLATE.format(
        path=VERSION_FILE.format(env=env),
        repository=repository,
        tag=tag,
        commit=yaml_scalar(commit),
        ref=yaml_scalar(ref),
        pipeline=yaml_scalar(pipeline),
    )


def _unquote(raw: str) -> str:
    raw = raw.strip()
    if raw.startswith('"'):
        end = raw.find('"', 1)
        return str(json.loads(raw[: end + 1])) if end > 0 else raw.strip('"')
    if raw.startswith("'"):
        end = raw.find("'", 1)
        return raw[1:end] if end > 0 else raw.strip("'")
    return raw.split(" #", 1)[0].strip()


def parse_version_yaml(text: str) -> dict[str, dict[str, str]]:
    """Lecture minimale (2 niveaux) du format §4, suffisante pour image.* et source.*.

    Pas de PyYAML : l'image du job est python:alpine nue, et le format est fixé par le contrat.
    """
    result: dict[str, dict[str, str]] = {}
    section: str | None = None
    for line in text.splitlines():
        stripped = line.split("#", 1)[0] if line.lstrip().startswith("#") else line
        if not stripped.strip():
            continue
        top = re.match(r"^([A-Za-z_][\w.-]*):\s*(#.*)?$", stripped)
        if top:
            section = top.group(1)
            result.setdefault(section, {})
            continue
        child = re.match(r"^\s+([A-Za-z_][\w.-]*):\s*(.*)$", stripped)
        if child and section is not None:
            result[section][child.group(1)] = _unquote(child.group(2))
        elif not line.startswith((" ", "\t")):
            section = None
    return result


# --------------------------------------------------------------------------- client GitLab
class GitLabAPIError(PromoteError):
    def __init__(self, method: str, path: str, status: int, body: str) -> None:
        super().__init__(f"{method} {path} -> HTTP {status}: {body[:500]}")
        self.status = status
        self.body = body


class GitLab:
    """Mini-client API v4 (urllib). Le token ne quitte jamais l'en-tête PRIVATE-TOKEN."""

    def __init__(self, api_url: str, token: str) -> None:
        if not api_url.startswith(("http://", "https://")):
            raise PromoteError(f"URL d'API invalide : {api_url!r}")
        self.api_url = api_url.rstrip("/")
        self._token = token

    def request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        body: dict[str, Any] | None = None,
        raw: bool = False,
    ) -> Any:
        url = self.api_url + path
        if params:
            url += "?" + urllib.parse.urlencode(params, doseq=True)
        data = json.dumps(body).encode() if body is not None else None
        headers = {"PRIVATE-TOKEN": self._token, "Accept": "application/json"}
        if data is not None:
            headers["Content-Type"] = "application/json"
        last_error: Exception | None = None
        for attempt in range(1, 4):
            req = urllib.request.Request(url, data=data, method=method, headers=headers)  # noqa: S310 (schéma vérifié)
            try:
                with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT_S) as resp:  # noqa: S310
                    payload = resp.read().decode()
                    if raw:
                        return payload
                    return json.loads(payload) if payload else None
            except urllib.error.HTTPError as exc:
                text = exc.read().decode(errors="replace")
                if exc.code >= 500 and attempt < 3:
                    last_error = GitLabAPIError(method, path, exc.code, text)
                    time.sleep(2 * attempt)
                    continue
                raise GitLabAPIError(method, path, exc.code, text) from None
            except (urllib.error.URLError, TimeoutError, ConnectionError) as exc:
                last_error = exc
                if attempt < 3:
                    time.sleep(2 * attempt)
                    continue
        raise PromoteError(f"{method} {path} : GitLab injoignable ({last_error})")

    def get_or_none(self, path: str, **kwargs: Any) -> Any:
        try:
            return self.request("GET", path, **kwargs)
        except GitLabAPIError as exc:
            if exc.status == 404:
                return None
            raise


def q(value: str) -> str:
    """Encode un identifiant de chemin pour l'API (gitops/manifests -> gitops%2Fmanifests)."""
    return urllib.parse.quote(value, safe="")


# --------------------------------------------------------------------------- promotion
@dataclass
class Plan:
    env: str
    repository: str
    tag: str
    ref: str
    commit: str
    pipeline_url: str
    project: str
    auto_merge: bool
    commit_url: str

    @property
    def file_path(self) -> str:
        return VERSION_FILE.format(env=self.env)

    @property
    def branch(self) -> str:
        return BRANCH_PREFIX.format(env=self.env) + self.tag

    @property
    def title(self) -> str:
        return f"chore(notes-api): {self.env} -> {self.tag}"

    @property
    def image(self) -> str:
        return f"{self.repository}:{self.tag}"

    def render(self) -> str:
        return render_version_yaml(
            self.env, self.repository, self.tag, self.commit, self.ref, self.pipeline_url
        )

    def description(self) -> str:
        merge_line = (
            "Merge **automatique** dès que la pipeline de cette MR est verte (environnement dev)."
            if self.auto_merge
            else "**Merge humain** : le lead relit puis merge (Argo CD déploie ensuite)."
        )
        commit = f"[`{self.commit[:8]}`]({self.commit_url})" if self.commit_url else self.commit
        pipeline = f"[pipeline]({self.pipeline_url})" if self.pipeline_url else "—"
        return (
            f"Promotion de **notes-api** en **{self.env}**.\n\n"
            f"| | |\n|---|---|\n"
            f"| Image | `{self.image}` |\n"
            f"| Ref | `{self.ref}` |\n"
            f"| Commit | {commit} |\n"
            f"| Pipeline source | {pipeline} |\n"
            f"| Fichier | `{self.file_path}` |\n\n"
            f"{merge_line}\n\n"
            "_MR générée par `ci/promote.py` (gitops/notes-api) — ne pas modifier à la main._\n"
        )


def current_state(gl: GitLab, plan: Plan, ref: str) -> tuple[str | None, dict[str, dict[str, str]]]:
    raw = gl.get_or_none(
        f"/projects/{q(plan.project)}/repository/files/{q(plan.file_path)}/raw",
        params={"ref": ref},
        raw=True,
    )
    return raw, (parse_version_yaml(raw) if raw is not None else {})


def is_up_to_date(parsed: dict[str, dict[str, str]], plan: Plan) -> bool:
    image = parsed.get("image", {})
    return image.get("tag") == plan.tag and image.get("repository") == plan.repository


def ensure_branch(gl: GitLab, plan: Plan) -> bool:
    """Crée la branche depuis main si besoin. Renvoie True si elle existait déjà."""
    path = f"/projects/{q(plan.project)}/repository/branches"
    if gl.get_or_none(f"{path}/{q(plan.branch)}") is not None:
        log(f"branche {plan.branch} : existe déjà, réutilisée")
        return True
    gl.request("POST", path, params={"branch": plan.branch, "ref": TARGET_BRANCH})
    log(f"branche {plan.branch} : créée depuis {TARGET_BRANCH}")
    return False


def commit_file(gl: GitLab, plan: Plan, content: str, exists_on_branch: bool) -> None:
    gl.request(
        "POST",
        f"/projects/{q(plan.project)}/repository/commits",
        body={
            "branch": plan.branch,
            "commit_message": f"{plan.title}\n\nImage: {plan.image}\nSource: {plan.pipeline_url}",
            "actions": [
                {
                    "action": "update" if exists_on_branch else "create",
                    "file_path": plan.file_path,
                    "content": content,
                }
            ],
        },
    )
    log(f"commit « {plan.title} » sur {plan.branch}")


def close_superseded(gl: GitLab, plan: Plan) -> None:
    """Ferme les MR de promotion plus anciennes du même environnement (elles sont remplacées)."""
    mrs = gl.request(
        "GET",
        f"/projects/{q(plan.project)}/merge_requests",
        params={
            "state": "opened",
            "target_branch": TARGET_BRANCH,
            "labels": f"{LABEL_PROMOTION},env::{plan.env}",
            "per_page": 100,
        },
    )
    prefix = BRANCH_PREFIX.format(env=plan.env)
    for mr in mrs or []:
        source = str(mr.get("source_branch", ""))
        if not source.startswith(prefix) or source == plan.branch:
            continue
        iid = mr["iid"]
        gl.request(
            "POST",
            f"/projects/{q(plan.project)}/merge_requests/{iid}/notes",
            body={"body": f"Remplacée par la promotion de `{plan.image}` ({plan.branch})."},
        )
        gl.request(
            "PUT",
            f"/projects/{q(plan.project)}/merge_requests/{iid}",
            body={"state_event": "close"},
        )
        try:
            gl.request("DELETE", f"/projects/{q(plan.project)}/repository/branches/{q(source)}")
        except GitLabAPIError as exc:
            log(f"[warn] suppression de la branche {source} impossible : HTTP {exc.status}")
        log(f"MR !{iid} ({source}) fermée : remplacée")


def ensure_merge_request(gl: GitLab, plan: Plan) -> dict[str, Any]:
    path = f"/projects/{q(plan.project)}/merge_requests"
    fields = {
        "title": plan.title,
        "description": plan.description(),
        "labels": f"{LABEL_PROMOTION},env::{plan.env}",
        "remove_source_branch": True,
        "squash": False,
    }
    existing = gl.request(
        "GET",
        path,
        params={"state": "opened", "source_branch": plan.branch, "target_branch": TARGET_BRANCH},
    )
    if existing:
        iid = existing[0]["iid"]
        mr: dict[str, Any] = gl.request("PUT", f"{path}/{iid}", body=fields)
        log(f"MR !{iid} mise à jour : {mr.get('web_url', '')}")
        return mr
    mr = gl.request(
        "POST",
        path,
        body={"source_branch": plan.branch, "target_branch": TARGET_BRANCH, **fields},
    )
    log(f"MR !{mr['iid']} créée : {mr.get('web_url', '')}")
    return mr


def auto_merge(gl: GitLab, plan: Plan, iid: int) -> None:
    """Merge quand la pipeline réussit (ou tout de suite si elle a déjà réussi)."""
    mr_path = f"/projects/{q(plan.project)}/merge_requests/{iid}"
    deadline = time.monotonic() + HEAD_PIPELINE_TIMEOUT_S
    mr: dict[str, Any] = gl.request("GET", mr_path)
    while not mr.get("head_pipeline") and mr.get("state") == "opened":
        if time.monotonic() > deadline:
            log("[warn] toujours pas de pipeline sur la MR : tentative de merge quand même")
            break
        log("attente de la pipeline de la MR...")
        time.sleep(POLL_INTERVAL_S)
        mr = gl.request("GET", mr_path)
    if mr.get("state") == "merged":
        log(f"MR !{iid} déjà mergée")
        return

    deadline = time.monotonic() + MERGE_RETRY_TIMEOUT_S
    while True:
        pipeline = mr.get("head_pipeline") or {}
        status = pipeline.get("status")
        if status in {"failed", "canceled"}:
            raise PromoteError(
                f"la pipeline de la MR !{iid} est en échec ({status}) : pas de merge"
            )
        params: dict[str, Any] = {"should_remove_source_branch": True}
        if mr.get("sha"):
            params["sha"] = mr["sha"]
        if status == "success":
            mode = "merge immédiat (pipeline déjà verte)"
        else:
            # auto_merge (GitLab >= 17.11) et son ancien nom, toujours accepté par l'API v4.
            params.update({"merge_when_pipeline_succeeds": True, "auto_merge": True})
            mode = "merge automatique quand la pipeline réussit"
        try:
            result: dict[str, Any] = gl.request("PUT", f"{mr_path}/merge", body=params)
        except GitLabAPIError as exc:
            # 405/406/422 : MR pas encore « mergeable » (pipeline en création, vérifs en cours...)
            if exc.status in {405, 406, 409, 422} and time.monotonic() < deadline:
                log(f"merge refusé pour l'instant (HTTP {exc.status}), nouvel essai...")
                time.sleep(POLL_INTERVAL_S)
                mr = gl.request("GET", mr_path)
                if mr.get("state") == "merged":
                    log(f"MR !{iid} mergée")
                    return
                continue
            raise
        if result.get("state") == "merged":
            log(f"MR !{iid} mergée ({mode})")
        elif result.get("merge_when_pipeline_succeeds") or result.get("auto_merge_enabled"):
            log(f"MR !{iid} : {mode} activé")
        else:
            log(f"[warn] réponse inattendue au merge de la MR !{iid} : state={result.get('state')}")
        return


def run(plan: Plan, gl: GitLab | None, dry_run: bool) -> int:
    content = plan.render()
    log(f"environnement : {plan.env}")
    log(f"image         : {plan.image}")
    log(f"projet        : {plan.project} ({plan.file_path} sur {TARGET_BRANCH})")
    log(f"branche / MR  : {plan.branch} -> {TARGET_BRANCH} « {plan.title} »")
    log(f"auto-merge    : {'oui' if plan.auto_merge else 'non (merge humain)'}")

    if dry_run:
        if gl is not None:
            _, parsed = current_state(gl, plan, TARGET_BRANCH)
            current = parsed.get("image", {}).get("tag")
            log(f"tag actuel sur {TARGET_BRANCH} : {current!r}")
            if is_up_to_date(parsed, plan):
                log("déjà à jour : rien ne serait fait")
        else:
            log("pas de token/API : état actuel non lu")
        log("--dry-run : aucune écriture. Fichier qui serait commité :")
        print(content, end="", flush=True)
        return 0

    assert gl is not None  # noqa: S101 (garanti par main())
    raw, parsed = current_state(gl, plan, TARGET_BRANCH)
    if is_up_to_date(parsed, plan):
        log(f"{plan.file_path} référence déjà {plan.image} sur {TARGET_BRANCH} : rien à faire")
        return 0
    log(f"tag actuel sur {TARGET_BRANCH} : {parsed.get('image', {}).get('tag')!r}")

    close_superseded(gl, plan)
    branch_existed = ensure_branch(gl, plan)
    on_branch = raw
    if branch_existed:
        on_branch, _ = current_state(gl, plan, plan.branch)
    if on_branch == content:
        log(f"{plan.branch} contient déjà le bon fichier : pas de nouveau commit")
    else:
        commit_file(gl, plan, content, exists_on_branch=on_branch is not None)

    mr = ensure_merge_request(gl, plan)
    if plan.auto_merge:
        auto_merge(gl, plan, int(mr["iid"]))
    else:
        log("pas d'auto-merge : la MR attend la relecture et le merge du lead")
    return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    env = os.environ
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument("--env", required=True, help="environnement cible (dev, prod)")
    p.add_argument("--repository", required=True, help="dépôt d'image, sans tag")
    p.add_argument("--tag", required=True, help="tag d'image à déployer")
    p.add_argument("--ref", default=env.get("CI_COMMIT_REF_NAME", ""), help="branche ou tag git")
    p.add_argument("--commit", default=env.get("CI_COMMIT_SHA", ""), help="sha complet")
    p.add_argument("--pipeline-url", default=env.get("CI_PIPELINE_URL", ""))
    p.add_argument("--project", default=env.get("MANIFESTS_PROJECT", "gitops/manifests"))
    p.add_argument("--api-url", default=env.get("CI_API_V4_URL", ""))
    p.add_argument(
        "--project-url", default=env.get("CI_PROJECT_URL", ""), help="pour le lien commit"
    )
    p.add_argument("--auto-merge", action="store_true", help="merge quand la pipeline réussit")
    p.add_argument("--dry-run", action="store_true", help="affiche le plan, n'écrit rien")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    if hasattr(sys.stdout, "reconfigure"):  # accents lisibles même sur une console Windows
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    args = parse_args(argv)
    if not ENV_RE.match(args.env):
        raise PromoteError(f"--env invalide : {args.env!r}")
    if not TAG_RE.match(args.tag):
        raise PromoteError(f"--tag invalide : {args.tag!r}")
    if not REPOSITORY_RE.match(args.repository):
        raise PromoteError(f"--repository invalide : {args.repository!r}")
    commit_url = (
        f"{args.project_url.rstrip('/')}/-/commit/{args.commit}" if args.project_url else ""
    )
    plan = Plan(
        env=args.env,
        repository=args.repository,
        tag=args.tag,
        ref=args.ref,
        commit=args.commit,
        pipeline_url=args.pipeline_url,
        project=args.project,
        auto_merge=args.auto_merge,
        commit_url=commit_url if args.commit else "",
    )
    token = os.environ.get("MANIFESTS_TOKEN", "")
    gl = GitLab(args.api_url, token) if token and args.api_url else None
    if gl is None and not args.dry_run:
        raise PromoteError("MANIFESTS_TOKEN et CI_API_V4_URL (ou --api-url) sont requis")
    return run(plan, gl, args.dry_run)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except PromoteError as error:
        log(f"ERREUR : {error}")
        sys.exit(1)
