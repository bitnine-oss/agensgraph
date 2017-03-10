Cutting a new release
---------------------

**NOTE:** This assumes you've run the tests and they pass.

1. Increase the version number.
    * [ ] Change the name of `hll--X.Y.Z.sql`.
    * [ ] Change `hll.control`.
    * [ ] Change `Makefile`.
    * [ ] Change `postgresql-hll.spec`.
    * [ ] Change `README.markdown`
2. Edit changelogs in `postgresql-hll.spec` and `CHANGELOG.markdown`.
3. Commit those two sets of changes.
4. Tag the commit. 'vX.Y.Z' is the name format.
5. Push tag and commits to `master`.
