# Summary

[summary]: summary

4Paradigm's coding convention apply to C/CPP, Java, Scala, Python, Yaml, Json, Shell Script.

# Motivation

[motivation]: motivation

Archive better code quality by:

- Enforce well-known code convention like Google's Style to main programing languages
- Use linter check tools and bootstrap those tools in automatic CICD check

# Detailed design

[detailed-design]: detailed-design

## Overview

[overview]: overview

In summary, imperative languages (C/CPP, Java, Scala, Python), is based on Google's Style Guide if have, with a bit adjustment:

- indent: 4 space
- max line length: 120 character
- name convention: we follow Google's naming convention which is camel case. One thing pointed out here is that for abbreviations, only capitalized the first letter. e.g., `Start RPC` should name as `StartRpc`

Concrete style guide can be found:

- [Google CPP Style Guide](https://google.github.io/styleguide/cppguide.html)
- [Google Java Style Guide](https://google.github.io/styleguide/javaguide.html)
- [Google Python Style Guide](https://google.github.io/styleguide/pyguide.html)

Data-Serialization language like yaml/json, should follow:

- indent: 2 space
- max line length: 120 character
- prefer double quote (json excluded)

## Suggested linter and formater

By default, appropriate linters should setup in CICD to enforce convention in place like Pull Request. Formater is usually used in local development to quickly resolve some style issues. There is no need to use exactly same tools and configuration, any tools respect [overview](#overview) rules should suffice.

| language  | linter                                               | linter config                                                                      | formater                                                           | format config                                                                  |
| --------- | ---------------------------------------------------- | ---------------------------------------------------------------------------------- | ------------------------------------------------------------------ | ------------------------------------------------------------------------------ |
| cpp       | [cpplint](https://github.com/cpplint/cpplint)        | default config                                                                     | [clang-format](https://clang.llvm.org/docs/ClangFormat.html)       | [.clang-format](https://github.com/4paradigm/HybridSE/blob/main/.clang-format) |
| java      | [checkstyle](https://checkstyle.sourceforge.io/)     | [style.xml](https://github.com/4paradigm/HybridSE/blob/main/java/style_checks.xml) | [eclipse.jdt](https://github.com/eclipse/eclipse.jdt.core) | [eclipse-formater.xml](https://github.com/4paradigm/HybridSE/blob/main/java/eclipse-formatter.xml)                                                        |
| scala | [scalastyle](http://www.scalastyle.org/) | [scala_style.xml](https://github.com/4paradigm/NativeSpark/blob/main/native-spark/scala_style.xml) | [scalafmt](https://scalameta.org/scalafmt) | |
| python    | [pylint](https://www.pylint.org/)                    | [pylintrc](https://github.com/4paradigm/HybridSE/blob/main/pylintrc)               | [yapf](https://github.com/google/yapf)                             | default with google style                                                      |
| yaml/json | -                                                    | -                                                                                  | [prettier](https://prettier.io/)                                   | [prettierrc](https://github.com/4paradigm/HybridSE/blob/main/.prettierrc.yml)  |
| shell     | [shellcheck](https://github.com/koalaman/shellcheck) | default                                                                            | [shfmt](https://github.com/mvdan/sh)                               | default with indednt = 4                                                       |

formater do not always solve lint error, dig hard to the lint rule.

### Cpp

[style-cpp]: style-cpp

- linter: cpplint, with `linelength=120`
- formater: clang-format and [.clang-format](https://github.com/4paradigm/HybridSE/blob/main/.clang-format) based on Google style config

### java

- linter: checkstyle
  - use template [google_checks.xml](https://github.com/checkstyle/checkstyle/blob/master/src/main/resources/google_checks.xml)
  - checkstyle maven plugin: <https://maven.apache.org/plugins/maven-checkstyle-plugin/>
- formater: [spotless](https://github.com/diffplug/spotless/tree/master/plugin-maven) provide eclipse.jdt integration.

### Scala

- linter: scalastyle with [maven plugin](http://www.scalastyle.org/maven.html)
- formater: scalafmt with [spotless](https://github.com/diffplug/spotless/tree/master/plugin-maven)

### Python

- pylint: based on config provided by google: [pylintrc](https://google.github.io/styleguide/pylintrc)
- yapf: a sample yapf config: [style.yapf](https://github.com/4paradigm/HybridSE/blob/main/.style.yapf)

### Yaml/Json/Xml

- config in [prettierrc](https://github.com/4paradigm/HybridSE/blob/feat/style-and-doc/.prettierrc.yml)
  - tabWidth: 2
  - singleQuote: false
  - printWidth: 120

### Shell Script

- follow shellcheck rules: <https://github.com/koalaman/shellcheck/wiki/Checks>
- follow shfmt format rules: <https://github.com/mvdan/sh/blob/master/cmd/shfmt/shfmt.1.scd>

### doxygen

- doxygen comment rules: <https://www.doxygen.nl/manual/docblocks.html>
- doxygen markdown support: <https://www.doxygen.nl/manual/markdown.html>
- c++ comment style: <https://github.com/4paradigm/HybridSE/discussions/27#discussioncomment-546319>
- java/scala comment style: @tobegit3hub
- python comment style: @tobegit3hub
