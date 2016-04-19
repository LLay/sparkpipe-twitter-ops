# Contributing to Sparkpipe Twitter Ops

## Rules

1. **PR everything**. Commits made directly to master are prohibited, except under specific circumstances
1. **Use feature branches**. Create an issue for every single feature or bug and tag it. If you are a core contributor, create a branch named feature/[issue #] to resolve the issue. If you are not, fork and branch.
1. Use github "Fixes #[issue]" syntax on your PRs to indicate which issues you are attempting to resolve
1. Code coverage is strictly enforced at 100%, so don't worry if your build fails prior to PRing. Just make sure you bring the coverage back up as part of your final PR.
1. Please try to follow the Scala coding style exemplified by existing source files

## Developing and Testing

Since testing Sparkpipe requires a Spark cluster, a containerized development/test environment is included via [Docker](https://www.docker.com/). If you have docker installed, you can interactively build and test Sparkpipe within that environment:

```bash
$ ./test-environment
# then, inside the running container
$ ./gradlew
```

This will mount the code directory into the container as a volume, allowing you to make code changes on your host machine and test them on-the-fly.

You can also debug the test suite using remote debugging on port 9999. If you're on linux, the host for the debugger will be localhost. If you're on Windows or OSX, then it'll be the ip of your docker toolbox VM.

```bash
# start the test container as before
$ ./test-environment
# then, inside the running container
$ ./gradlew debug
# You'll see a message indicating when you should connect your debugger.
```
