# Summary

[summary]: #summary

Convention for PR/Issue/Commits on GitHub

# Motivation

[motivation]: motivation

To standardize workflows in GitHub. Create a clean and mantainable code space.

# Guide-level explanation

[guide-level-explanation]: #guide-level-explanation

## Commit

[commit]: #commit

1. commit pushed to default branch (merge commit exclude) must follow [conventional commit](https://www.conventionalcommits.org/en/v1.0.0/)
2. every commit pushed must sign with gpg, and shows verified signature in GitHub page
3. commiter user: use a valid GitHub username to commit as much as you can
4. force push is prohibited in most case, it should use only for security patch
5. idealy, we encourage conventional commit in every related branch, also name branch with similar style, e.g `fix/resolve-core-dump`

rule 1 and 2 is enforced.

## Issue

[issue]: #issue

Follow the issue template defined in each project, which should at least have

- bug report tempate: e.g [bug report](https://github.com/memark-io/pmedis/blob/main/.github/ISSUE_TEMPLATE/bug_report.md)
- feature request template

## Pull Request

[pr]: #pr

1. merge action.
   - `merge`, `squash merge` and `rebase merge` are all allowed.
   - maintainer should decide the right action, not mess up the commit tree.
2. PR Template
   - A Pull Request Template like [PR template](https://github.com/memark-io/pmedis/blob/main/.github/pull_request_template.md) should provided.
3. Conditions to suffice before merge
   - reach pre-defined approval number
   - CI all pass
   - performance and coverage report reach expectations (exclude doc changes)

## Branch

[branch]: branch

- use branch name `main` as default branch
- default branch is protected, commit should not push to default branch directly, follow the fork-pr-merge progress

## Tag and Release

[tag-and-release]: tag-and-release

- use [semantic versioning](https://semver.org)
- a semantic version tag should start with a `v` prefix, e.g `v1.0.1`
- semantic version tags (start with `v` prefix) is immutable, force push is prohibited.
- release asserts should provide DIGESTS{.asc,} file as well, which contains hashes and gpg sign

# Reference-level explanation

[reference-level-explanation]: #reference-level-explanation

## Conventional Commits Reference

[conventional-commits-ref]: #coventional-commits-ref

Complete rules available in

- https://conventional-changelog.github.io/commitlint/#/reference-rules
- the [angular style](https://github.com/angular/angular.js/blob/master/DEVELOPERS.md#-git-commit-guidelines) give more detail description

Here is a basic summary.

A conventional commit looks like:

```txt
<type>[optional scope]: <subject>
# empty line
[optional body]
# empty line
[optional footer(s)]
```

Different project can propose different types based on project content.
Convention commit define `<type>` of follow types:

```txt
  'build',    // Changes that affect the build system or external dependencies (example scopes: gulp, broccoli, npm)
  'chore',    // Other changes that don't modify src or test files
  'ci',       // Changes to our CI configuration files and scripts (example scopes: Travis, Circle, BrowserStac k, SauceLabs)
  'docs',     // Documentation only changes
  'feat',     // A new feature
  'fix',      // A bug fix
  'perf',     // A code change that improves performance
  'refactor', // A code change that neither fixes a bug nor adds a feature
  'revert',   // Reverts a previous commit
  'style',    // Changes that do not affect the meaning of the code (white-space, formating)
  'test'      // Adding missing tests or correcting existing tests
```

`scope` can be modified aspect or issue number.

E.g.

```txt
feat(12): add windows support
```

# Other

## conventional commit tool

[conventional-commit-tool]: conventional-commit-tool

Tools can be used to enforce conventional commit locally and remotely.

Enforce conventional style locally, use

- [husky](https://github.com/typicode/husky): git hook tool
- [commitlint](https://github.com/conventional-changelog/commitlint): commit message linter
- [commitizen](https://github.com/commitizen/cz-cli): command line tool to write conventional style commits

Detail usage also available in #23

Enforce conventional style remotely, use GitHub action to archive it in PR

- [pr name lint action](https://github.com/JulienKode/pull-request-name-linter-action)
- [commit lint action](https://github.com/wagoid/commitlint-github-action)
