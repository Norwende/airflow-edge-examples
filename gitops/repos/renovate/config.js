// Configuration globale du bot Renovate auto-hébergé (CONTRACT.md §6).
// Lue par le job `renovate` (RENOVATE_CONFIG_FILE=config.js). Le token vient de la variable CI
// RENOVATE_TOKEN (PAT de l'utilisateur renovate-bot), jamais de ce fichier.
module.exports = {
  platform: 'gitlab',
  endpoint: process.env.RENOVATE_ENDPOINT || 'http://gitlab.127-0-0-1.sslip.io/api/v4',

  // Découvre tous les projets du groupe gitops auxquels renovate-bot a accès.
  autodiscover: true,
  autodiscoverFilter: ['gitops/*'],

  // Pas de MR d'onboarding : un projet sans renovate.json utilise le preset ci-dessous.
  onboarding: false,
  requireConfig: 'optional',

  gitAuthor: 'Renovate Bot <renovate-bot@lab.local>',

  // Pas de token GitHub dans le lab : pas de récupération des release notes / changelogs.
  fetchChangeLogs: 'off',

  // Image -full : uv, helm, node/npm... déjà installés.
  binarySource: 'global',

  // Cache de dépôt (accélère les exécutions horaires ; conservé par le cache GitLab du job).
  repositoryCache: 'enabled',
};
