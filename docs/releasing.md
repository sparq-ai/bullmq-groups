# Releasing

How `@sparq/bullmq-groups` gets to npm.

## Two names, and they are not the same

| | Value | What it is |
| --- | --- | --- |
| npm scope | `@sparq` | what `npm install` resolves |
| GitHub org | `sparq-ai` | what the trusted publisher binds to |

Confusing them sends you to the wrong settings page. The npm package is `@sparq/bullmq-groups`; the repository is `sparq-ai/bullmq-groups`.

## Making a release

Bump the version and merge to `main`. That is the whole process.

```bash
# on a branch
npm version patch --no-git-tag-version   # or edit package.json + package-lock.json
git commit -am "Bump the version to 0.1.5"
# open a PR, merge it
```

On every push to `main`, `.github/workflows/npm-publish.yml` runs typecheck, tests and build, then compares `package.json`'s version against npm:

| Version on `main` | What happens |
| --- | --- |
| already on npm | `should_publish=false`, publish skipped, job green |
| new | `npm publish --access=public --provenance` |

So a merge that does not touch the version is a clean no-op, and you never publish by accident.

Tests run on pull requests only (`ci.yml`), so a merge does not re-run what the PR already proved.

## Authentication

There is **no `NPM_TOKEN` secret**. Publishing uses npm [Trusted Publishing](https://docs.npmjs.com/trusted-publishers) over OIDC: GitHub Actions proves its identity to npm directly, and npm checks it against a publisher you configured once.

Three things this depends on, each of which breaks the publish if changed:

- **`permissions: id-token: write`** in the workflow. OIDC cannot mint a token without it.
- **`NODE_AUTH_TOKEN` must stay unset.** If it is defined — even empty — npm sends that empty string instead of falling back to OIDC, and the publish fails with an auth error that does not mention OIDC at all.
- **`environment: production`** on the job must match the environment named in the trusted publisher.

A successful OIDC publish attaches a provenance attestation, which is how you tell an automated publish from a manual one:

```bash
npm view @sparq/bullmq-groups@0.1.4 dist.attestations
```

A version published by hand has none.

## First-time setup

Already done for this package. Kept because it is not obvious and it is ordering-sensitive.

npm binds a trusted publisher to a package that **already exists**. A brand-new package therefore cannot be published by CI — there is nothing for OIDC to attach to, so the first push would run `npm publish --provenance` against a 404 and fail.

The bootstrap, in order:

1. **Publish once by hand**, from a checkout carrying the new version:
   ```bash
   npm login
   npm publish --access=public
   ```
   No `--provenance` — OIDC is not wired yet, and this is the step that makes wiring it possible.

   `--access=public` is **mandatory**. Scoped packages default to restricted, and a restricted publish silently makes the package private.

2. **Configure the trusted publisher** — npmjs.com → `@sparq/bullmq-groups` → Settings → Trusted publisher → GitHub Actions:

   | Field | Value |
   | --- | --- |
   | Owner | `sparq-ai` |
   | Repository | `bullmq-groups` |
   | Workflow | `npm-publish.yml` |
   | Environment | `production` |

   The binding is repository + workflow + environment. It does **not** name a branch, so the publishing branch can be whatever the workflow says.

3. **Create the `production` environment** — repo Settings → Environments. The job declares `environment: production`; if it is missing, or carries required reviewers, the publish stalls or fails.

4. **Only then push to `main`.** The version is already on npm by now, so the version check skips the publish and the run is green.

## Checking a release

The npmjs.com web page caches for several minutes and will lag behind a publish. These do not:

```bash
npm view @sparq/bullmq-groups version        # what npm install resolves
curl -s https://registry.npmjs.org/@sparq%2fbullmq-groups/latest | jq -r .version
```

`https://img.shields.io/npm/v/@sparq/bullmq-groups` renders the live version if you want something visual.

## Things that will bite you

- **A published version can never be republished.** Unpublish is allowed within 72 hours and nothing after. Getting a version wrong means burning it and bumping again.
- **Nothing gates the publish on the compat matrix.** `npm-publish.yml` runs `npm test` against the pinned BullMQ in `devDependencies`. It does not run `test:min` (5.40.0) or `test:v6` (6.3.15), although the declared peer range is `>=5.40.0 <7`. A change that breaks either end of the range still publishes. Giving the publish job a `needs:` on the matrix is the fix.
- **`files: ["dist"]`** in `package.json` decides the tarball. Docs, tests and benchmarks are not shipped. Check with `npm pack --dry-run` before changing it.
- **`prepublishOnly` runs `typecheck` and `build`**, so a manual publish cannot ship a stale `dist/`.

## Deprecating a version

```bash
npm deprecate @sparq/bullmq-groups@0.1.3 "Superseded by 0.1.4"
npm deprecate bullmq-groups "Moved to @sparq/bullmq-groups"
```

The second is the old unscoped package, published before the move to the org scope.
