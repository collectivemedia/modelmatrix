# Model Matrix

Model Matrix (feature engineering tools)

### Git Workflow

This repository workflow is based on [A successful Git branching model](http://nvie.com/posts/a-successful-git-branching-model/) with two main branches with an infinite lifetime:

* master
* develop

The **master** branch at origin should be familiar to every Git user. Parallel to the master branch, another branch exists called **develop**.

We consider **origin/master** to be the main branch where the source code of HEAD always reflects a production-ready state.

We consider **origin/develop** to be the main branch where the source code of HEAD always reflects a state with the latest delivered development changes for the next release. Some would call this the “integration branch”. This is where any automatic nightly builds are built from.

Further details are available in [A successful Git branching model](http://nvie.com/posts/a-successful-git-branching-model/) blog post.
