1. always use base package name se.afshin.yavari
2. Try to keep the reconcilers clean so break out business logic into other classes
3. tests
  - Always write unit tests and keep them updated when refactoring
  - run make teardown and mcs setup
  - always run e2e tests, the fast version
  - I will manually run e2e full/extended tests, or explicitly tell you to run them
4. Update docs, architecture, api-reference, readme, operations after every feature
5. Every new feature should be implemented in a feature branch
6. commit, push and create a PR after tests have passed 
