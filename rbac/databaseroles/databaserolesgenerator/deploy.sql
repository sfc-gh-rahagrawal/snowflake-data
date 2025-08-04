ALTER GIT REPOSITORY DB_GOVERNANCE.REPO.SF_GITHUB_REPO FETCH;

LS @DB_GOVERNANCE.REPO.SF_GITHUB_REPO/branches/main/rbac/databaseroles/databaserolesgenerator;

CREATE OR REPLACE NOTEBOOK DB_GOVERNANCE.APPS."DatabaseRolesGenerator"
FROM @DB_GOVERNANCE.REPO.SF_GITHUB_REPO/branches/main/rbac/databaseroles/databaserolesgenerator/notebook
MAIN_FILE = "DatabaseRolesGenerator.ipynb"
QUERY_WAREHOUSE = "COMPUTE_WH";