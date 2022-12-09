[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/DACH-NY/projection/blob/main/LICENSE)

Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0

# Projection

A Java / Scala projection library for projecting Ledger events to SQL databases.
(Currently only PostgreSQL is supported.)

A **Projection** is a resumable process that continuously reads ledger events and transforms these into rows in SQL tables.
A projection ensures that rows are committed according to ledger transactions,
ensuring that isolation and atomicity of changes perceived by database users is consistent with committed transactions on the ledger.

## Reference documentation

The reference documentation is available [at docs.daml.com](https://docs.daml.com/2.5.0/app-dev/custom-views/index.html).

## Examples
A quickstart example using the projection library can be found [here](https://github.com/digital-asset/ex-custom-views-spring-boot).

## Releasing

This project is set up to create a snapshot on Maven for every `main` commit.
To make a stable release, merge a commit on main with the

```
deploy: true
tag: <version>
```

trailers, for example:

```
git commit -m "release 2.4.6

deploy: true
tag: 2.4.6
```

Note that what matters is the commit message that ends up on `main`, so be
careful when merging a PR that intends to create a release.

## Community

We feel that a welcoming community is important, and we ask that you follow our [Code of Conduct](./CODE_OF_CONDUCT.md) in all interactions with the community.
You can join these groups and chats to discuss and ask questions:

- Issue tracker: [![github: DACH-NY/projection](https://img.shields.io/badge/github%3A-issues-blue.svg?style=flat-square)](https://github.com/DACH-NY/projection/issues)
- Forum: [discuss.daml.com](https://discuss.daml.com)

## Contributing

**Contributions are *very* welcome!**

If you see an issue that you'd like to see fixed, or if you want to explore ideas,
the best way to make it happen is to help out by submitting a pull request that implements it.
We welcome contributions from all, even if you are not yet familiar with this project.
We are happy to get you started, and will guide you through the process once you've submitted your PR.

In general pull requests should be submitted against main. See [CONTRIBUTING.md](./CONTRIBUTING.md) for more details about how to contribute.

