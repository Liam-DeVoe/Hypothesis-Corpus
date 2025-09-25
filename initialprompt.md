I'm a phd researcher. I want to investigate a research question with you, with the eventual goal of writing a paper on the data we collect. Here are some emails which provide context for the problem:

```
When I visited Northeastern last month, you asked me what sorts of things I'd want to hear about from your PBT corpus analysis study. I finally managed to put together a list with the help of Benjamin and Leo; here's what I've got:
Properties — What kinds of properties? Are they primarily mathematical, model-based, round-trip, etc.? Are properties separate from the normal test suite or integrated?
Generators — What built-in generators are used most often? How often are people writing their own generators (and what are they)? How often are people using assume in a way that might hurt performance? How often are generators using effects (e.g., accessing a database)?
Analysis — How often are people using events or other tools for tracking test performance?
Testing Methodologies — How often are people using hypofuzz, CrossHair, or other ways of running their properties besides pure random?
Obviously I expect you'll focus on a set of things that isn't exactly aligned with this list, but if there was anything on this list that I'd be especially excited for you to pay attention to it's the stuff about generators (/ strategies).

---
This is a wonderful list, thanks Harry!

We're relatively set on having:
  * Some open coding of property types, with the possibility of novel categories
  * Distribution of generator usage, including composition (e.g. both st.lists(st.integers()) and explicit st.composite).
  * How often do people use note / event / target / assume?
  * How do people run their pbts? What runner (tox / pytest / nose), where are the tests/ files relative to src/, do they have distinct commands for the pbt portion of their test suite?

Anything more fine grained is very up for consideration, and you have some great leads and additions here. I'm especially interested in assume performance and effects, the latter not being something I'd considered. I have seen a surprising number of effects in this IR study evaluation and they have been the bane of my parallelism performance.
```

I don't want to dive in to implementing the full experiment yet. What I do want to do is prototype the experimental setup so I can see what works and what doesn't work. A few things:
- It's important to me that I have visibility into the results so I can visually inspect, check in, and understand the work and results as it progresses. To that end, I would like to integrate with some sort of open dashboard framework as an integral part of this work, so I can easily and efficiently view experimental results and observability. Propose a few solutions here.
- I already have a dataset of ~30k property-based tests (stored as github url + pytest nodeid + venv/pip requirements.txt file) which will form the basis of the dataset we evaluate on. For some metrics, we will need to install the repo and run the tests. I am imagining a setup where we spin up one worker per core, and each worker takes a repository, clones it, installs its requirements in a venv, then collects + writes data. The repository should be run in some sort of isolated enviornment like a docker container (though I'm not tied to docker if there are better alternatives).

Use the following sample item from the dataset to test the full workflow: `{"MarkCBell/bigger": {"node_ids": ["tests/structures.py::TestUnionFind::runTest"], "requirements.txt": "attrs==24.2.0\nexceptiongroup==1.2.2\nhypothesis==6.112.5\niniconfig==2.0.0\npackaging==24.1\npillow==11.0.0\npluggy==1.5.0\npytest==8.2.2\nsortedcontainers==2.4.0\ntomli==2.0.2"}`
