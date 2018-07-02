For pull requests please include the following:

- [ ] closes #xxxx
- [ ] tests added / passed
- [ ] passes `git diff upstream/master -u -- "*.py" | flake8 --diff`
- [ ] whatsnew entry

### Notes:

To run the `git diff` on the `upstream/master` branch make sure this is set
up, e.g.

```
git remote add upstream git@github.com:matthewgilbert/pdblp.git
git fetch upstream
```

See [here](https://stackoverflow.com/questions/9257533/what-is-the-difference-between-origin-and-upstream-on-github)
for more details.

Unfortunately automated testing using TravisCI is not possible since many tests
require a valid Bloomberg connection. Please verify that
`pytest pdblp/tests -v` has been run and all tests are passing.