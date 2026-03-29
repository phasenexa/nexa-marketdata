Run the following checks:

1. Code should not be needless complex, or duplicated. Review and make sure DRY and elegant aligned with requirements
   - Refactor if necessary
   - Approach should be consistent across all concrete implementations
2. All tests pass using `make ci` (lint, typechecks, test and testnotebooks etc.)
3. Execute all notebooks `make execute-notebooks`
4. Test coverage should be >80%
5. Review the README file, check nothing major is missing, add additions if something is identified
